package repos

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/sourcegraph/sourcegraph/pkg/api"
	"github.com/sourcegraph/sourcegraph/pkg/conf"
	"github.com/sourcegraph/sourcegraph/pkg/gitserver"
	"github.com/sourcegraph/sourcegraph/pkg/mutablelimiter"
	log15 "gopkg.in/inconshreveable/log15.v2"
)

const (
	// minDelay is the minimum amount of time between scheduled updates for a single repository.
	minDelay = 45 * time.Second

	// maxDelay is the maximum amount of time between scheduled updates for a single repository.
	maxDelay = 8 * time.Hour
)

// updateScheduler schedules repo update (or clone) requests to gitserver.
//
// Repository metadata is synced from configured code hosts and added to the scheduler.
//
// Updates are scheduled based on the time that has elapsed since the last commit
// divided by a constant factor of 2. For example, if a repo's last commit was 8 hours ago
// then the next update will be scheduled 4 hours from now. If there are still no new commits,
// then the next update will be scheduled 6 hours from then.
// This heuristic is simple to compute and has nice backoff properties.
//
// When it is time for a repo to update, the scheduler inserts the repo into a queue.
//
// A worker continuously dequeues repos and sends updates to gitserver, but its concurrency
// is limited by the gitMaxConcurrentClones site configuration.
type updateScheduler struct {
	mu sync.Mutex

	// confRepos stores the last known list of repos from each source
	// so we can compute which repos have been added/removed/enabled/disabled.
	confRepos map[string]sourceRepoMap

	updateQueue *updateQueue
	schedule    *schedule
}

// sourceRepoMap is the set of repositories associated with a specific configuration source.
type sourceRepoMap map[api.RepoURI]*configuredRepo

// newUpdateScheduler returns a new scheduler.
func newUpdateScheduler() *updateScheduler {
	return &updateScheduler{
		confRepos: make(map[string]sourceRepoMap),
		updateQueue: &updateQueue{
			notifyEnqueue: make(chan struct{}, 1),
		},
		schedule: &schedule{
			wakeup: make(chan struct{}, 1),
		},
	}
}

// run starts scheduled repo updates.
func (s *updateScheduler) run(ctx context.Context) {
	go s.runScheduleLoop(ctx)
	go s.runUpdateLoop(ctx)
}

// runScheduleLoop starts the loop that schedules updates by enqueuing them into the updateQueue.
func (s *updateScheduler) runScheduleLoop(ctx context.Context) {
	for {
		select {
		case <-s.schedule.wakeup:
		case <-ctx.Done():
			return
		}

		s.schedule.mu.Lock()

		for {
			if len(s.schedule.heap) == 0 {
				break
			}

			repoUpdate := s.schedule.heap[0]
			if !repoUpdate.due.Before(timeNow().Add(time.Second)) {
				break
			}

			s.updateQueue.enqueue(repoUpdate.repo, priorityLow)
			repoUpdate.due = dueTimeFromNow(repoUpdate.interval)
			heap.Fix(s.schedule, 0)
		}

		s.schedule.rescheduleTimer()
		s.schedule.mu.Unlock()
	}
}

// runUpdateLoop sends repo update requests to gitserver.
func (s *updateScheduler) runUpdateLoop(ctx context.Context) {
	limiter := mutablelimiter.New(1)
	conf.Watch(func() {
		limit := conf.Get().GitMaxConcurrentClones
		if limit == 0 {
			limit = 5
		}
		limiter.SetLimit(limit)
	})

	for {
		select {
		case <-s.updateQueue.notifyEnqueue:
		case <-ctx.Done():
			return
		}

		for repo := s.updateQueue.acquireNext(); repo != nil; {
			ctx, cancel, err := limiter.Acquire(ctx)
			if err != nil {
				// context is canceled; shutdown
				return
			}
			go func(ctx context.Context, repo *configuredRepo, cancel context.CancelFunc) {
				defer cancel()
				defer s.updateQueue.remove(repo)

				resp, err := gitserver.DefaultClient.RequestRepoUpdate(ctx, gitserver.Repo{Name: repo.uri, URL: repo.url}, 1*time.Second)
				if err != nil {
					log15.Warn("error requesting repo update", "uri", repo.uri, "err", err)
				}
				if resp.LastFetched != nil && resp.LastChanged != nil {
					// This is the heuristic that is described in the updateScheduler documentation.
					// Update that documentation if you update this logic.
					interval := resp.LastFetched.Sub(*resp.LastChanged) / 2
					s.schedule.update(repo, interval)
				}
			}(ctx, repo, cancel)
		}
	}
}

// updateSource updates the list of configured repos associated with the given source.
// This is the source of truth for what repos exist in the schedule.
func (s *updateScheduler) updateSource(source string, newList sourceRepoMap) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.confRepos[source] == nil {
		s.confRepos[source] = sourceRepoMap{}
	}

	// Remove repos that don't exist in the new list or are disabled in the new list.
	oldList := s.confRepos[source]
	for key, repo := range oldList {
		if updatedRepo, ok := newList[key]; !ok || !updatedRepo.enabled {
			s.schedule.remove(repo)
			s.updateQueue.remove(repo)
		}
	}

	// Schedule enabled repos.
	for key, updatedRepo := range newList {
		if !updatedRepo.enabled {
			continue
		}

		oldRepo := oldList[key]
		if oldRepo == nil || !oldRepo.enabled {
			s.schedule.add(updatedRepo)
			s.updateQueue.enqueue(updatedRepo, priorityLow)
		}
	}

	s.confRepos[source] = newList
}

// UpdateOnce causes a single update of the given repository.
// It neither adds nor removes the repo from the schedule.
func (s *updateScheduler) UpdateOnce(uri api.RepoURI, url string) {
	repo := &configuredRepo{
		uri: uri,
		url: url,
	}
	s.updateQueue.enqueue(repo, priorityHigh)
}

// updateQueue is a priority queue of repos to update.
// A repo can't have more than one location in the queue.
type updateQueue struct {
	mu sync.Mutex

	heap  []*repoUpdate
	index map[api.RepoURI]*repoUpdate

	// The queue performs a non-blocking send on this channel
	// when a new value is enqueued so that the update loop
	// can wake up if it is idle.
	notifyEnqueue chan struct{}
}

type priority int

const (
	priorityLow priority = iota
	priorityHigh
)

// repoUpdate is a repository that has been queued for an update.
type repoUpdate struct {
	repo       *configuredRepo
	priority   priority
	insertTime time.Time // the time that the update was inserted into the queue
	updating   bool      // whether the repo has been acquired for update
	index      int       // the index in the heap
}

// enqueue add the repo to the queue with the given priority.
func (q *updateQueue) enqueue(repo *configuredRepo, p priority) {
	q.mu.Lock()
	defer q.mu.Unlock()

	update := q.index[repo.uri]
	if update == nil {
		heap.Push(q, &repoUpdate{
			repo:     repo,
			priority: p,
		})
		notify(q.notifyEnqueue)
		return
	}

	if p <= update.priority {
		// Repo is already in the queue with at least as good priority.
		return
	}

	// Repo is in the queue at a lower priority.
	update.priority = p           // bump the priority
	update.insertTime = timeNow() // put it after all existing updates with this priority
	heap.Fix(q, update.index)
	notify(q.notifyEnqueue)
}

// remove removes the repo from the queue.
func (q *updateQueue) remove(repo *configuredRepo) {
	q.mu.Lock()
	if update := q.index[repo.uri]; update != nil {
		heap.Remove(q, update.index)
	}
	q.mu.Unlock()
}

// acquireNext acquires the next repo for update.
// The acquired repo must be removed from the queue
// when the update finishes (independent of success or failure).
func (q *updateQueue) acquireNext() *configuredRepo {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.heap) == 0 {
		return nil
	}
	update := q.heap[0]
	if update.updating {
		// Everything in the queue is already updating.
		return nil
	}
	update.updating = true
	heap.Fix(q, update.index)
	return update.repo
}

// The following methods implement heap.Interface based on the priority queue example:
// https://golang.org/pkg/container/heap/#example__priorityQueue

func (q *updateQueue) Len() int { return len(q.heap) }
func (q *updateQueue) Less(i, j int) bool {
	qi := q.heap[i]
	qj := q.heap[i]
	if qi.updating != qj.updating {
		// Repos that are already updating are sorted last.
		return qj.updating
	}
	if qi.priority != qj.priority {
		// We want Pop to give us the highest, not lowest, priority so we use greater than here.
		return qi.priority > qj.priority
	}
	// Queue semantics for items with the same priority.
	return qi.insertTime.Before(qj.insertTime)
}
func (q *updateQueue) Swap(i, j int) {
	q.heap[i], q.heap[j] = q.heap[j], q.heap[i]
	q.heap[i].index = i
	q.heap[j].index = j
}
func (q *updateQueue) Push(x interface{}) {
	n := len(q.heap)
	item := x.(*repoUpdate)
	item.index = n
	item.insertTime = timeNow()
	q.heap = append(q.heap, item)
	q.index[item.repo.uri] = item
}
func (q *updateQueue) Pop() interface{} {
	n := len(q.heap)
	item := q.heap[n-1]
	item.index = -1 // for safety
	q.heap = q.heap[0 : n-1]
	delete(q.index, item.repo.uri)
	return item
}

// schedule is the schedule of when repos get enqueued into the updateQueue.
type schedule struct {
	mu sync.Mutex

	heap  []*scheduledRepoUpdate // min heap of scheduledRepoUpdates based on their due time.
	index map[api.RepoURI]*scheduledRepoUpdate

	// timer sends a value on the wakeup channel when it is time
	timer  *time.Timer
	wakeup chan struct{}
}

// scheduledRepoUpdate is the update schedule for a single repo.
type scheduledRepoUpdate struct {
	interval time.Duration   // how regularly the repo is updated
	due      time.Time       // the next time that the repo will be enqueued for a update
	repo     *configuredRepo // the repo to update
	index    int             // the index in the heap
}

func dueTimeFromNow(interval time.Duration) time.Time {
	return timeNow().Add(clamp(interval, minDelay, maxDelay))
}

func clamp(val, min, max time.Duration) time.Duration {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

// add adds a repo to the schedule.
// It does nothing if the repo already exists in the schedule.
func (s *schedule) add(repo *configuredRepo) {
	s.mu.Lock()
	if s.index[repo.uri] == nil {
		heap.Push(s, &scheduledRepoUpdate{
			repo:     repo,
			interval: minDelay,
			due:      dueTimeFromNow(minDelay),
		})
		s.rescheduleTimer()
	}
	s.mu.Unlock()
}

// update updates the update interval of a repo in the schedule.
// It does nothing if the repo is no longer in the schedule.
func (s *schedule) update(repo *configuredRepo, interval time.Duration) {
	s.mu.Lock()
	if update := s.index[repo.uri]; update != nil {
		update.interval = interval
		update.due = dueTimeFromNow(interval)
		heap.Fix(s, update.index)
		s.rescheduleTimer()
	}
	s.mu.Unlock()
}

// remove removes a repo from the schedule.
func (s *schedule) remove(repo *configuredRepo) {
	s.mu.Lock()
	if update := s.index[repo.uri]; update != nil {
		heap.Remove(s, update.index)
		if update.index == 0 {
			s.rescheduleTimer()
		}
	}
	s.mu.Unlock()
}

// rescheduleTimer schedules the scheduler to wakeup
// at the time that the next repo is due for an update.
// The caller must hold the lock on s.mu.
func (s *schedule) rescheduleTimer() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	if len(s.heap) > 0 {
		delay := s.heap[0].due.Sub(timeNow())
		s.timer = timeAfterFunc(delay, func() {
			notify(s.wakeup)
		})
	}
}

// The following methods implement heap.Interface based on the priority queue example:
// https://golang.org/pkg/container/heap/#example__priorityQueue

func (s *schedule) Len() int { return len(s.heap) }
func (s *schedule) Less(i, j int) bool {
	return s.heap[i].due.Before(s.heap[j].due)
}
func (s *schedule) Swap(i, j int) {
	s.heap[i], s.heap[j] = s.heap[j], s.heap[i]
	s.heap[i].index = i
	s.heap[j].index = j
}
func (s *schedule) Push(x interface{}) {
	n := len(s.heap)
	item := x.(*scheduledRepoUpdate)
	item.index = n
	s.heap = append(s.heap, item)
	s.index[item.repo.uri] = item
}
func (s *schedule) Pop() interface{} {
	n := len(s.heap)
	item := s.heap[n-1]
	item.index = -1 // for safety
	s.heap = s.heap[0 : n-1]
	delete(s.index, item.repo.uri)
	return item
}

// notify performs a non-blocking send on the channel.
// The channel should be buffered.
func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Mockable time functions for testing.
var (
	timeNow       = time.Now
	timeAfterFunc = time.AfterFunc
)
