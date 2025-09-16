module Api::V1
  class AsyncTasksController < ApiController

    DEFAULT_PER_PAGE = 20

    before_action :set_pagination_params
    include PaperTrailControllerInfo

    def index
      scope = AsyncTask.scope_for_user(current_user, current_org, params[:all].truthy?)
      scope = scope.by_status(params[:status].downcase) if params[:status]
      @async_tasks = scope.page(@page).per_page(@per_page)
      set_link_header(@async_tasks)
    end

    def types
      render json: AsyncTasks::Manager::ALLOWED_TASK_CLASSES
    end

    def show
      @async_task = AsyncTask.find_for_user(current_user, current_org, params[:id])
    end

    def create
      input = (validate_body_json AsyncTask).symbolize_keys
      @async_task = AsyncTask.build_from_input(current_user, current_org, input, request)
      AsyncTasks::Manager.start_job!(@async_task)
      render :show
    end

    def rerun
      @async_task = AsyncTask.find_for_user(current_user, current_org, params[:id])
      raise ActiveRecord::RecordNotFound unless @async_task
      if @async_task.running?
        raise Api::V1::ApiError.new(:bad_request, "Task is already running")
      end

      @async_task = @async_task.dup
      @async_task.assign_attributes(
        status: :pending,
        retries_count: nil,
        error: nil,
        result: nil,
        progress: 0,
        started_at: nil,
        stopped_at: nil,
        acknowledged_at: nil,
        result_purged: false,
        should_be_killed: nil
      )
      @async_task.save!

      AsyncTasks::Manager.start_job!(@async_task)
      @async_task.reload
      render :show
    end

    def result
      async_task = AsyncTask.find_for_user(current_user, current_org, params[:id])
      return head :processing if async_task.running? || async_task.pending?

      raise Api::V1::ApiError.new(:bad_request, async_task.error) if async_task.error?
      raise Api::V1::ApiError.new(:bad_request, 'Task is cancelled') if async_task.cancelled?

      render json: async_task.result
    end

    def explain_arguments
      task_type = params[:type]
      task_class = AsyncTasks::Manager.task_class(task_type)
      render json: task_class.new(nil).explain_arguments
    end

    def by_status
      scope = AsyncTask.scope_for_user(current_user, current_org, params[:all].truthy?).by_status(params[:status])
      @async_tasks = scope.page(@page).per_page(@per_page)
      set_link_header(@async_tasks)
      render :index
    end

    def of_type
      scope = AsyncTask.scope_for_user(current_user, current_org, params[:all].truthy?).of_type(params[:type])
      if params[:status]
        scope = scope.by_status(params[:status])
      end
      @async_tasks = scope.page(@page).per_page(@per_page)
      set_link_header(@async_tasks)
      render :index
    end

    def download_link
      task = AsyncTask.find_for_user(current_user, current_org, params[:id])

      instance = AsyncTasks::Manager.instantiate_task(task)
      raise Api::V1::ApiError.new(:bad_request, "Task isn't completed") unless task.completed?
      raise Api::V1::ApiError.new(:bad_request, "Task doesn't support download") unless instance.provides_download_link?
      raise Api::V1::ApiError.new(:bad_request, "Result was purged") if task.result_purged?

      url = task.generate_presigned_url!

      render plain: url
    end

    def acknowledge
      @async_task = AsyncTask.find_for_user(current_user, current_org, params[:id])
      @async_task.update(acknowledged_at: Time.now, result_purged: true)
      instance = AsyncTasks::Manager.instantiate_task(@async_task)
      instance.purge_result
      render :show
    end

    def destroy
      task = AsyncTask.find_for_user(current_user, current_org, params[:id])
      if task.running?
        raise Api::V1::ApiError.new(:bad_request, "Task is running")
      end
      task.destroy
      head :ok
    end

    private
    def set_pagination_params
      @page = params[:page] || 1
      @per_page = params[:per_page] || DEFAULT_PER_PAGE
      @paginate = true
    end

  end
end
