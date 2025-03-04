package concurrency

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"log/slog"

	"go-workers/utils"
	"golang.org/x/time/rate"
)

const (
	TimedOutErr = "jobs got timed out"
)

type (
	Workers struct {
		size        int
		maxduration time.Duration
	}

	Worker struct {
		tasks  <-chan *kvPair
		work   *Work
		signal *signal
	}

	Work struct {
		workfn     func(context.Context, any) (any, error)
		cancelFlag bool
		limiter    *rate.Limiter
	}

	RateLimit struct {
		rps   rate.Limit
		burst int
	}

	signal struct {
		ctx    context.Context
		cancel context.CancelFunc
	}

	kvPair struct {
		k string
		v any
	}
)

var kvPairPool = sync.Pool{
	New: func() any {
		return &kvPair{}
	},
}

func init() {
	// Pre-warm the pool
	for i := 0; i < 10; i++ {
		kvPairPool.Put(&kvPair{})
	}
}

func NewWorkers(size int, maxDur time.Duration) *Workers {
	return &Workers{
		size:        size,
		maxduration: maxDur,
	}
}

func NewWork(workfn func(context.Context, any) (any, error), cancelFlag bool) *Work {
	return &Work{
		workfn:     workfn,
		cancelFlag: cancelFlag,
	}
}

func NewLimiterWork(workfn func(context.Context, any) (any, error), cancelFlag bool, rl *RateLimit) *Work {
	return &Work{
		workfn:     workfn,
		cancelFlag: cancelFlag,
		limiter:    rate.NewLimiter(rl.rps, rl.burst),
	}
}

func NewRateLimit(rps rate.Limit, burst int) *RateLimit {
	return &RateLimit{
		rps:   rps,
		burst: burst,
	}
}

func (w Workers) Run(parentCtx context.Context, jobs map[string]any, work *Work) (map[string]any, error) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(parentCtx, timeoutDur(w.maxduration)) // merge with parent context
	signal := &signal{
		ctx:    ctx,
		cancel: cancel,
	}
	defer signal.cancel()

	buffer := len(jobs)
	if buffer > 1000 {
		buffer = 1000
	}

	tasks := make(chan *kvPair, buffer)
	results := make(chan *kvPair, buffer)

	for i := 0; i < w.size; i++ {
		wg.Add(1)
		Worker{
			tasks:  tasks,
			work:   work,
			signal: signal,
		}.start(&wg, results)
	}

	collect := make(chan map[string]any)
	quitCollect := make(chan any)

	go accumulate(results, collect, quitCollect)
	go distribute(ctx, jobs, tasks)
	go func() {
		wg.Wait()
		close(results) // close results once all the workers are done
	}()

	select {
	case v := <-collect:
		return v, nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			close(quitCollect)
			return nil, fmt.Errorf(TimedOutErr)
		}

		// workers cancelled on error so return whatever collected so far.
		return <-collect, nil
	}
}

// collects the completed job results
func accumulate(results <-chan *kvPair, collect chan<- map[string]any, quit <-chan any) {
	defer close(collect)
	r := map[string]any{}
	for result := range results { // non-blocking read till the buffer is empty.
		r[result.k] = result.v

		// reset the kvpair & return it back to the pool.
		result.k = ""
		result.v = nil
		kvPairPool.Put(result)
	}
	select {
	case collect <- r:
	case <-quit:
	}
}

// pushes all the jobs to tasks channel
func distribute(ctx context.Context, jobs map[string]any, tasks chan<- *kvPair) {
	defer close(tasks)
	for name, job := range jobs {
		task := kvPairPool.Get().(*kvPair)
		task.k = name
		task.v = job

		select {
		case tasks <- task: // non-blocking write till the buffer exceeds.
		case <-ctx.Done():
			task.k = ""
			task.v = nil

			kvPairPool.Put(task) // return to pool
			return               // stop pushing jobs if cancelled or timed out.
		}
	}
}

// worker picks the tasks, calls work func & pushes the results
func (w Worker) start(wg *sync.WaitGroup, results chan<- *kvPair) {
	go func() {
		defer func() {
			wg.Done()
			if r := recover(); r != nil {
				slog.Error(
					utils.RecoveryErr(r).Error(),
					"func", "concurrency.Worker.Start()",
					"step", "processing.tasks",
					"stack", runtime.StartTrace(),
				)
			}
		}()

		for task := range w.tasks { // non-blocking read till buffer is empty
			select {
			case <-w.signal.ctx.Done(): // for communication between workers
				return
			default:
			}

			if w.work.limiter != nil {
				if err := w.work.limiter.Wait(w.signal.ctx); err != nil {
					return
				}
			}

			if r, err := w.work.workfn(w.signal.ctx, task.v); err != nil {
				task.v = err // resuse the task itself to update the result
				results <- task
				if w.work.cancelFlag {
					w.signal.cancel() // sends a signal to all the workers
					continue
				}
			} else {
				task.v = r // resuse the task itself to update the result
				results <- task
			}
		}
	}()
}

func timeoutDur(maxDuration time.Duration) time.Duration {
	if maxDuration > 0 {
		return maxDuration
	}
	return 1 * time.Minute
}
