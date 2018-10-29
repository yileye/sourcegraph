// Package scheduler controls when update requests are sent to gitserver.
//
// Repository metadata is synced from configured code hosts and added to the scheduler.
//
// The scheduler schedules updates based on the time that has elapsed since the last commit
// divided by a constant factor of 2. For example, if a repo's last commit was 8 hours ago
// then the next update will be scheduled 4 hours from now. If there are still no new commits,
// then the next update will be scheduled 6 hours from then.
// This heuristic is simple to compute and has nice backoff properties.
//
// When it is time for a repo to update, the scheduler inserts the repo into a queue.
//
// A worker continuously dequeues repos and sends updates to gitserver, but its concurrency
// is limited by the gitMaxConcurrentClones site configuration.
package scheduler

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

// repoQueue is a queue of repos to update.
// There are two priority levels: priority and background.
// A repo can't have more than one location in the queue.
type repoQueue struct {
	mu sync.Mutex

	priority      []*ConfiguredRepo
	priorityIndex map[api.RepoURI]int

	background      []*ConfiguredRepo
	backgroundIndex map[api.RepoURI]int

	// The queue performs a non-blocking send on this channel
	// when a new value is enqueued so that the update loop
	// can wake up if it is idle.
	notifyEnqueue chan struct{}
}

// enqueueBackground enqueues the repo into the background queue.
// It does nothing if the repo already exists in the priority or background queue.
func (q *repoQueue) enqueueBackground(repo *ConfiguredRepo) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.priorityIndex[repo.URI]; ok {
		return
	}
	if _, ok := q.backgroundIndex[repo.URI]; ok {
		return
	}

	q.backgroundIndex[repo.URI] = len(q.background)
	q.background = append(q.background, repo)
	notify(q.notifyEnqueue)
}

// enqueuePriority enqueues the repo into the priority queue
// and removes it from the background queue.
// It does nothing if the repo already exists in the priority queue.
func (q *repoQueue) enqueuePriority(repo *ConfiguredRepo) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.priorityIndex[repo.URI]; ok {
		// Already in the priority queue.
		return
	}

	q.removeBackground(repo.URI)
	q.priorityIndex[repo.URI] = len(q.priority)
	q.priority = append(q.priority, repo)
	notify(q.notifyEnqueue)
}

// remove removes the repo from both the priority and background queues.
func (q *repoQueue) remove(repo *ConfiguredRepo) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.removeBackground(repo.URI)
	if idx, ok := q.priorityIndex[repo.URI]; ok {
		q.priority = append(q.priority[:idx], q.priority[idx+1:]...)
		delete(q.priorityIndex, repo.URI)
	}
}

// removeBackground removes the repo from the background queue.
// The caller must hold the lock for q.mu.
func (q *repoQueue) removeBackground(uri api.RepoURI) {
	if idx, ok := q.backgroundIndex[uri]; ok {
		q.background = append(q.background[:idx], q.background[idx+1:]...)
		delete(q.backgroundIndex, uri)
	}
}

// dequeue dequeues a repo from the priority queue if it is not empty.
// Otherwise, it dequeues from the background queue instead.
func (q *repoQueue) dequeue() *ConfiguredRepo {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.priority) > 0 {
		front := q.priority[0]
		q.priority = q.priority[1:]
		delete(q.priorityIndex, front.URI)
		return front
	}

	if len(q.background) > 0 {
		front := q.background[0]
		q.background = q.background[1:]
		delete(q.backgroundIndex, front.URI)
		return front
	}
	return nil
}

// ConfiguredRepo is a repository from a configuration source.
type ConfiguredRepo struct {
	URI     api.RepoURI
	URL     string
	Enabled bool
}

// SourceRepoList is the set of repositories associated with a specific configuration source.
type SourceRepoList map[api.RepoURI]*ConfiguredRepo

// Repos holds all of the state.
var Repos = repoList{
	confRepos: make(map[string]SourceRepoList),
	queue: repoQueue{
		notifyEnqueue: make(chan struct{}, 1),
	},
	scheduler: scheduler{
		wakeup: make(chan struct{}, 1),
	},
}

// Run starts scheduled repo updates.
func Run(ctx context.Context) {
	go Repos.runScheduleLoop(ctx)
	go Repos.runUpdateLoop(ctx)
}

type repoList struct {
	mu sync.Mutex

	// confRepos stores the last known list of repos from each source
	// so we can compute which repos have been added/removed/enabled/disabled.
	confRepos map[string]SourceRepoList

	queue     repoQueue
	scheduler scheduler
}

// UpdateOnce causes a single update of the given repository.
// It neither adds nor removes the repo from the scheduler.
func (r *repoList) UpdateOnce(uri api.RepoURI, url string) {
	repo := &ConfiguredRepo{
		URI: uri,
		URL: url,
	}
	r.queue.enqueuePriority(repo)
}

// UpdateSource updates the list of configured repos associated with the given source.
// This is the source of truth for what repos exist in the scheduler.
func (r *repoList) UpdateSource(source string, newList SourceRepoList) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.confRepos[source] == nil {
		r.confRepos[source] = SourceRepoList{}
	}

	// Remove repos that don't exist in the new list or are disabled in the new list.
	oldList := r.confRepos[source]
	for key, repo := range oldList {
		if updatedRepo, ok := newList[key]; !ok || !updatedRepo.Enabled {
			r.scheduler.remove(repo)
			r.queue.remove(repo)
		}
	}

	// Schedule enabled repos.
	for key, updatedRepo := range newList {
		if !updatedRepo.Enabled {
			continue
		}

		oldRepo := oldList[key]
		if oldRepo == nil || !oldRepo.Enabled {
			r.scheduler.add(updatedRepo)
			r.queue.enqueueBackground(updatedRepo)
		}
	}

	r.confRepos[source] = newList
}

// runUpdateLoop sends repo update requests to gitserver.
func (r *repoList) runUpdateLoop(ctx context.Context) {
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
		case <-r.queue.notifyEnqueue:
		case <-ctx.Done():
			return
		}

		for repo := r.queue.dequeue(); repo != nil; {
			ctx, cancel, err := limiter.Acquire(ctx)
			if err != nil {
				// context is canceled; shutdown
				return
			}
			go func(ctx context.Context, repo *ConfiguredRepo, cancel context.CancelFunc) {
				defer cancel()
				resp, err := gitserver.DefaultClient.RequestRepoUpdate(ctx, gitserver.Repo{Name: repo.URI, URL: repo.URL}, 1*time.Second)
				if err != nil {
					log15.Warn("error requesting repo update", "uri", repo.URI, "err", err)
				}
				if resp.LastFetched != nil && resp.LastChanged != nil {
					// This is the heuristic that is described in the package documentation.
					// Update that documentation if you update this logic.
					interval := resp.LastFetched.Sub(*resp.LastChanged) / 2
					r.scheduler.update(repo, interval)
				}
			}(ctx, repo, cancel)
		}
	}
}

// scheduler schedules when repos are enqueued for update.
type scheduler struct {
	mu       sync.Mutex
	schedule schedule

	// timer ticks when the scheduler should wakeup next
	timer  *time.Timer
	wakeup chan struct{}
}

// scheduledRepoUpdate is the update schedule for a single repo.
type scheduledRepoUpdate struct {
	interval time.Duration   // how regularly the repo is updated
	due      time.Time       // the next time that the repo will be enqueued for a update
	repo     *ConfiguredRepo // the repo to update
	index    int             // the index in the heap
}

func dueTimeFromNow(interval time.Duration) time.Time {
	return time.Now().Add(clamp(interval, minDelay, maxDelay))
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

// add adds a repo to the scheduler.
// It does nothing if the repo already exists in the scheduler.
func (s *scheduler) add(repo *ConfiguredRepo) {
	s.mu.Lock()
	if i := s.getIndex(repo); i == -1 {
		heap.Push(&s.schedule, &scheduledRepoUpdate{
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
func (s *scheduler) update(repo *ConfiguredRepo, interval time.Duration) {
	s.mu.Lock()
	if i := s.getIndex(repo); i != -1 {
		s.schedule[i].interval = interval
		s.schedule[i].due = dueTimeFromNow(interval)
		heap.Fix(&s.schedule, i)
		s.rescheduleTimer()
	}
	s.mu.Unlock()
}

// remove removes a repo from the schedule.
func (s *scheduler) remove(repo *ConfiguredRepo) {
	s.mu.Lock()
	if i := s.getIndex(repo); i != -1 {
		heap.Remove(&s.schedule, i)
		if i == 0 {
			s.rescheduleTimer()
		}
	}
	s.mu.Unlock()
}

// rescheduleTimer schedules the scheduler to wakeup
// at the time that the next repo is due for an update.
// The caller must hold the lock on s.mu.
func (s *scheduler) rescheduleTimer() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	if len(s.schedule) > 0 {
		delay := time.Until(s.schedule[0].due)
		s.timer = time.AfterFunc(delay, func() {
			notify(s.wakeup)
		})
	}
}

func (s *scheduler) getIndex(repo *ConfiguredRepo) int {
	// TODO: This is O(n) when it could be O(1) with a proper map.
	for i, r := range s.schedule {
		if r.repo.URI == repo.URI {
			return i
		}
	}
	return -1
}

// schedule is a min heap of scheduledRepoUpdates based on their due time.
// It is based on the priority queue example: https://golang.org/pkg/container/heap/#example__priorityQueue
type schedule []*scheduledRepoUpdate

func (p schedule) Len() int { return len(p) }
func (p schedule) Less(i, j int) bool {
	return p[i].due.Before(p[j].due)
}
func (p schedule) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].index = i
	p[j].index = j
}
func (p *schedule) Push(x interface{}) {
	n := len(*p)
	item := x.(*scheduledRepoUpdate)
	item.index = n
	*p = append(*p, item)
}
func (p *schedule) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*p = old[0 : n-1]
	return item
}

// runScheduleLoop schedules repos to be updated.
func (r *repoList) runScheduleLoop(ctx context.Context) {
	for {
		select {
		case <-r.scheduler.wakeup:
		case <-ctx.Done():
			return
		}

		r.scheduler.mu.Lock()

		for {
			if len(r.scheduler.schedule) == 0 {
				break
			}

			repoUpdate := r.scheduler.schedule[0]
			if !repoUpdate.due.Before(time.Now().Add(time.Second)) {
				break
			}

			r.queue.enqueueBackground(repoUpdate.repo)
			repoUpdate.due = dueTimeFromNow(repoUpdate.interval)
			heap.Fix(&r.scheduler.schedule, 0)
		}

		r.scheduler.rescheduleTimer()
		r.scheduler.mu.Unlock()
	}
}

// notify performs a non-blocking send on the channel.
// The channel should be buffered.
func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
