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

			s.updateQueue.enqueueBackground(repoUpdate.repo)
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

		for repo := s.updateQueue.dequeue(); repo != nil; {
			ctx, cancel, err := limiter.Acquire(ctx)
			if err != nil {
				// context is canceled; shutdown
				return
			}
			go func(ctx context.Context, repo *configuredRepo, cancel context.CancelFunc) {
				defer cancel()
				resp, err := gitserver.DefaultClient.RequestRepoUpdate(ctx, gitserver.Repo{Name: repo.uri, URL: repo.url}, 1*time.Second)
				if err != nil {
					log15.Warn("error requesting repo update", "uri", repo.uri, "err", err)
				}
				if resp.LastFetched != nil && resp.LastChanged != nil {
					// This is the heuristic that is described in the package documentation.
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
			s.updateQueue.enqueueBackground(updatedRepo)
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
	s.updateQueue.enqueuePriority(repo)
}

// updateQueue is a queue of repos to update.
// There are two priority levels: priority and background.
// A repo can't have more than one location in the queue.
type updateQueue struct {
	mu sync.Mutex

	priority      []*configuredRepo
	priorityIndex map[api.RepoURI]int

	background      []*configuredRepo
	backgroundIndex map[api.RepoURI]int

	// The queue performs a non-blocking send on this channel
	// when a new value is enqueued so that the update loop
	// can wake up if it is idle.
	notifyEnqueue chan struct{}
}

// enqueueBackground enqueues the repo into the background queue.
// It does nothing if the repo already exists in the priority or background queue.
func (q *updateQueue) enqueueBackground(repo *configuredRepo) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.priorityIndex[repo.uri]; ok {
		return
	}
	if _, ok := q.backgroundIndex[repo.uri]; ok {
		return
	}

	q.backgroundIndex[repo.uri] = len(q.background)
	q.background = append(q.background, repo)
	notify(q.notifyEnqueue)
}

// enqueuePriority enqueues the repo into the priority queue
// and removes it from the background queue.
// It does nothing if the repo already exists in the priority queue.
func (q *updateQueue) enqueuePriority(repo *configuredRepo) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.priorityIndex[repo.uri]; ok {
		// Already in the priority queue.
		return
	}

	q.removeBackground(repo.uri)
	q.priorityIndex[repo.uri] = len(q.priority)
	q.priority = append(q.priority, repo)
	notify(q.notifyEnqueue)
}

// remove removes the repo from both the priority and background queues.
func (q *updateQueue) remove(repo *configuredRepo) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.removeBackground(repo.uri)
	if idx, ok := q.priorityIndex[repo.uri]; ok {
		q.priority = append(q.priority[:idx], q.priority[idx+1:]...)
		delete(q.priorityIndex, repo.uri)
	}
}

// removeBackground removes the repo from the background queue.
// The caller must hold the lock for q.mu.
func (q *updateQueue) removeBackground(uri api.RepoURI) {
	if idx, ok := q.backgroundIndex[uri]; ok {
		q.background = append(q.background[:idx], q.background[idx+1:]...)
		delete(q.backgroundIndex, uri)
	}
}

// dequeue dequeues a repo from the priority queue if it is not empty.
// Otherwise, it dequeues from the background queue instead.
func (q *updateQueue) dequeue() *configuredRepo {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.priority) > 0 {
		front := q.priority[0]
		q.priority = q.priority[1:]
		delete(q.priorityIndex, front.uri)
		return front
	}

	if len(q.background) > 0 {
		front := q.background[0]
		q.background = q.background[1:]
		delete(q.backgroundIndex, front.uri)
		return front
	}
	return nil
}

// sourceRepoMap is the set of repositories associated with a specific configuration source.
type sourceRepoMap map[api.RepoURI]*configuredRepo

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
