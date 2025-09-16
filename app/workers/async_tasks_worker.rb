class AsyncTasksWorker
  include Sidekiq::Worker

  def perform(task_id)
    async_task = AsyncTask.find(task_id)
    instance = AsyncTasks::Manager.instantiate_task(async_task)
    instance.perform
  end
end