package utils

import (
	"context"
	"sync"
)

// TODO: Use task queue for procesisng requestss

// Task represents a unit of work to be processed by the worker pool.
type Task struct {
	ID      int
	Request interface{}
	Process func(ctx context.Context, request interface{}) (interface{}, error)
}

// Result represents the result of a processed task.
type Result struct {
	ID     int
	Output interface{}
	Err    error
}

// WorkerPool manages a pool of workers to process tasks concurrently.
type WorkerPool struct {
	// Make it a buffered channel
	tasks chan Task
	// Make it a buffered channel
	results     chan Result
	workerCount int
	wg          sync.WaitGroup
}

// NewWorkerPool creates a new WorkerPool with the specified number of workers.
func NewWorkerPool(workerCount int) *WorkerPool {
	return &WorkerPool{
		tasks:       make(chan Task),
		results:     make(chan Result),
		workerCount: workerCount,
	}
}

// Start initializes the worker pool and begins processing tasks.
func (wp *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx)
	}
}

// AddTask adds a new task to the worker pool for processing.
func (wp *WorkerPool) AddTask(task Task) {
	wp.tasks <- task
}

// Results returns a channel to receive processed task results.
func (wp *WorkerPool) Results() <-chan Result {
	return wp.results
}

// Stop stops the worker pool gracefully after all tasks are processed.
func (wp *WorkerPool) Stop() {
	close(wp.tasks)
	wp.wg.Wait()
	close(wp.results)
}

func (wp *WorkerPool) worker(ctx context.Context) {
	defer wp.wg.Done()
	for task := range wp.tasks {
		output, err := task.Process(ctx, task.Request)
		wp.results <- Result{
			ID:     task.ID,
			Output: output,
			Err:    err,
		}
	}
}
