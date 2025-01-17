package mongo

import (
	"context"
	"mongopher-scheduler/task_scheduler/scheduler"
	"mongopher-scheduler/task_scheduler/store"
	"time"
)

func (ts *MongoTaskScheduler) WaitForAllTasksToBeInStatusWithOptions(status store.TaskStatus, options scheduler.WaitForTasksOptions) (bool, error) {
	context, cancel := context.WithTimeout(context.Background(), options.Timeout)
	defer cancel()

	ticker := time.NewTicker(options.RetryDelay)
	defer ticker.Stop()

	for {
		select {
		case <-context.Done():
			return false, context.Err()
		case <-ticker.C:
			tasks, err := ts.store.GetAllTasks(context)

			if err != nil {
				return false, err
			}

			all_in_status := true
			for _, task := range tasks {
				if task.Status != status {
					all_in_status = false
					break;
				}
			}

			if all_in_status {
				return true, nil
			}
		}
	}
}

func (ts *MongoTaskScheduler) WaitForAllTasksToBeDone() (bool, error) {
	options := scheduler.DefaultWaitForTasksOptions()
	return ts.WaitForAllTasksToBeDoneWithOptions(options)
}

func (ts *MongoTaskScheduler) WaitForAllTasksToBeDoneWithOptions (options scheduler.WaitForTasksOptions) (bool, error) {
	return ts.WaitForAllTasksToBeInStatusWithOptions(store.StatusDone, options)
}

func (ts *MongoTaskScheduler) WaitForAllTasksToBeInStatu(status store.TaskStatus) (bool, error) {
	options := scheduler.DefaultWaitForTasksOptions()
	return ts.WaitForAllTasksToBeInStatusWithOptions(status, options)
}
