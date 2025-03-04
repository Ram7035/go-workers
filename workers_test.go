package concurrency

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	jobtesting "go-workers/testing"
)

func TestLargeJob(t *testing.T) {
	defer jobtesting.Teardown(t)

	ngrBefore := runtime.NumGoroutine()
	t.Logf("No of go routines before worker run %v", ngrBefore)

	largeJob(t)

	ngrAfter := runtime.NumGoroutine()
	t.Logf("No of go routines after worker run %v", ngrAfter)
}

func TestLargeJobParallel(t *testing.T) {
	t.Parallel()
	largeJob(t)
}

func largeJob(t *testing.T) {
	jobs := map[string]any{}
	// 1 million jobs
	for i := 0; i < 1e6; i++ {
		name := "Job" + strconv.Itoa(i+1)
		jobs[name] = i + 1
	}

	workfn := func(ctx context.Context, v any) (any, error) {
		num := v.(int)
		val := num + 5
		return &val, nil
	}

	workers := NewWorkers(100, 0)
	work := NewWork(workfn, true)
	results, _ := workers.Run(context.Background(), jobs, work)
	if len(jobs) != len(results) {
		t.Errorf("The no of jobs is not matching the no of results")
	}

	var err string
	for k, v := range results {
		switch val := v.(type) {
		case *int:
			num := jobs[k].(int)
			if ((*val) - 5) != num {
				t.Errorf("The result is not satisfying the work function.")
			}
		case error:
			err = val.Error()
		}
	}

	if len(err) > 0 {
		t.Errorf("No error was returned by the work function but got error.")
	}
}

func TestBreakOnError(t *testing.T) {
	defer jobtesting.Teardown(t)

	ngrBefore := runtime.NumGoroutine()
	t.Logf("No of go routines before worker run %v", ngrBefore)

	breakOnError(t)

	ngrAfter := runtime.NumGoroutine()
	t.Logf("No of go routines after worker run %v", ngrAfter)
}

func TestBreakOnErrorParallel(t *testing.T) {
	t.Parallel()
	breakOnError(t)
}

func breakOnError(t *testing.T) {
	jobs := map[string]any{}
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("Job%v", i+1)
		jobs[name] = i + 1
	}

	workfn := func(ctx context.Context, v any) (any, error) {
		num := v.(int)
		if num == 5 {
			return nil, fmt.Errorf("error on half mark!")
		} else if num%2 != 0 {
			time.Sleep(2 * time.Second)
		}
		sqr := (num * num)
		return &sqr, nil
	}

	workers := NewWorkers(5, 0)
	work := NewWork(workfn, true)
	results, _ := workers.Run(context.Background(), jobs, work)

	t.Logf("no of jobs %v", len(jobs))
	t.Logf("no of results %v", len(results))

	lessResults := len(results) <= len(jobs)
	if !lessResults {
		t.Errorf("The no of jobs should not equal results since break on error is true")
	}

	var err string
	for k, v := range results {
		switch sqr := v.(type) {
		case *int:
			num := jobs[k].(int)
			if ((*sqr) / num) != num {
				t.Errorf("The result is not satisfying the work function.")
			}
		case error:
			err = sqr.Error()
		}
	}

	if err != "error on half mark!" {
		t.Errorf("Did not receive the error returned by the work function.")
	}
}

func TestContinueOnError(t *testing.T) {
	defer jobtesting.Teardown(t)

	ngrBefore := runtime.NumGoroutine()
	t.Logf("No of go routines before worker run %v", ngrBefore)

	continueOnError(t)
	time.Sleep(5 * time.Second)

	ngrAfter := runtime.NumGoroutine()
	t.Logf("No of go routines after worker run %v", ngrAfter)
}

func TestContinueOnErrorParallel(t *testing.T) {
	t.Parallel()
	continueOnError(t)
}

func continueOnError(t *testing.T) {
	jobs := map[string]any{}
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("Job%v", i+1)
		jobs[name] = i + 1
	}

	workfn := func(ctx context.Context, v any) (any, error) {
		num := v.(int)
		if num == 5 {
			return nil, fmt.Errorf("error on half mark!")
		}
		sqr := (num * num)
		return &sqr, nil
	}

	workers := NewWorkers(5, 0)
	work := NewWork(workfn, false)
	results, _ := workers.Run(context.Background(), jobs, work)

	if len(results) != len(jobs) {
		t.Errorf("The no of jobs should be equal to results since break on error is false")
	}

	var err string
	for k, v := range results {
		switch sqr := v.(type) {
		case *int:
			num := jobs[k].(int)
			if ((*sqr) / num) != num {
				t.Errorf("The result is not satisfying the work function.")
			}
		case error:
			err = sqr.Error()
		}
	}

	if err != "error on half mark!" {
		t.Errorf("Did not receive the error returned by the work function.")
	}
}

func TestTimeout(t *testing.T) {
	defer jobtesting.Teardown(t)

	ngrBefore := runtime.NumGoroutine()
	t.Logf("No of go routines before worker run %v", ngrBefore)

	workersTimeout(t)
	time.Sleep(20 * time.Second)

	ngrAfter := runtime.NumGoroutine()
	t.Logf("No of go routines after worker run %v", ngrAfter)
}

func TestTimeoutParallel(t *testing.T) {
	t.Parallel()
	workersTimeout(t)
}

func workersTimeout(t *testing.T) {
	jobs := map[string]any{}
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("Job%v", i+1)
		jobs[name] = i + 1
	}

	workfn := func(ctx context.Context, v any) (any, error) {
		num := v.(int)
		sqr := (num * num)
		if num > 15 && num%2 == 0 {
			time.Sleep(10 * time.Second)
			return &sqr, nil
		}
		return &sqr, nil
	}

	timeout := 10 * time.Second
	workers := NewWorkers(5, timeout)
	work := NewWork(workfn, false)
	_, err := workers.Run(context.Background(), jobs, work)

	if err.Error() != TimedOutErr {
		t.Errorf("Expected to timeout but it didn't")
	}
}

func TestRateLimitJobs(t *testing.T) {
	defer jobtesting.Teardown(t)

	ngrBefore := runtime.NumGoroutine()
	t.Logf("No of go routines before worker run %v", ngrBefore)

	rateLimitJobs(t)

	ngrAfter := runtime.NumGoroutine()
	t.Logf("No of go routines after worker run %v", ngrAfter)
}

func rateLimitJobs(t *testing.T) {
	jobs := map[string]any{}
	for i := 0; i < 10; i++ {
		name := "Job" + strconv.Itoa(i+1)
		jobs[name] = i + 1
	}

	startTimes := make([]time.Time, 0, len(jobs))
	mu := sync.Mutex{}

	workfn := func(ctx context.Context, v any) (any, error) {
		defer func() {
			mu.Lock()
			startTimes = append(startTimes, time.Now())
			mu.Unlock()
		}()

		num := v.(int)
		val := num + 5
		return &val, nil
	}

	burst := 5
	workers := NewWorkers(10, 0)
	work := NewLimiterWork(workfn, true, NewRateLimit(2, burst))
	results, _ := workers.Run(context.Background(), jobs, work)
	if len(jobs) != len(results) {
		t.Errorf("The no of jobs is not matching the no of results")
	}

	var err string
	for k, v := range results {
		switch val := v.(type) {
		case *int:
			num := jobs[k].(int)
			if ((*val) - 5) != num {
				t.Errorf("The result is not satisfying the work function.")
			}
		case error:
			err = val.Error()
		}
	}

	if len(err) > 0 {
		t.Errorf("No error was returned by the work function but got error.")
	}

	// Sort start times for analysis
	sort.Slice(startTimes, func(i, j int) bool {
		return startTimes[i].Before(startTimes[j])
	})

	// Validate total initial capacity (burst)
	initialPickedTasks := 1 // including 0th start time
	for i := 1; i < len(startTimes); i++ {
		// Our rps is 2 meaning every 0.5 second a task will be picked up.
		// But at 0th time the no of tasks picked will be equal to the burst size
		// & all those tasks will be picked before the 0.5 second mark.
		if startTimes[i].Sub(startTimes[0]) < 500*time.Millisecond {
			initialPickedTasks++
		} else {
			break
		}
	}

	if initialPickedTasks != burst {
		t.Errorf("Expected %d tasks to process immediately (burst), but got %d", burst, initialPickedTasks)
	}

	// Validate subsequent rate limit
	// We have set 2 rps meaning 1 request per 0.5 seconds. Initial burst would happen at 0s
	// & at 0.5 seconds the next task will be picked up & at 1s another task will be picked resulting in two rps.
	tolerance := 5 * time.Millisecond
	for i := initialPickedTasks + 1; i < len(startTimes); i++ {
		elapsed := startTimes[i].Sub(startTimes[i-1])
		expected := 500 * time.Millisecond
		if elapsed < expected-tolerance || elapsed > expected+tolerance {
			t.Errorf("A task has violated rate limit: elapsed = %v, expected ~500ms (±%v)", elapsed, tolerance)
		}
	}
}
