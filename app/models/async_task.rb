class AsyncTask < ApplicationRecord
  include Api::V1::Schema
  include JsonAccessor
  include AuditLog

  belongs_to :owner, class_name: 'User'
  belongs_to :org

  json_accessor :arguments, :result, :request_data

  enum status: { pending: 'pending', running: 'running', completed: 'completed', failed: 'failed', cancelled: 'cancelled' }

  scope :by_status, ->(status) { where(status: status) }
  scope :of_type, ->(type) { where(task_type: type) }

  def self.find_for_user(api_user, api_org, id)
    scope_for_user(api_user, api_org, true).find(id)
  end

  def self.scope_for_user(api_user, api_org, all=false)
    if all && api_user.super_user?
      AsyncTask.all
    elsif all && api_org.has_admin_access?(api_user)
      AsyncTask.where(org_id: api_org.id)
    else
      AsyncTask.where(owner_id: api_user.id)
    end
  end

  def self.build_from_input(api_user, api_org, input, request)
    unless input[:task_type].present?
      raise Api::V1::ApiError.new(:bad_request, 'task_type is required')
    end

    AsyncTask.create(
      owner_id: api_user.id,
      org_id: api_org.id,
      task_type: input[:task_type],
      priority: input[:priority],
      arguments: input[:arguments],
      status: :pending,
      request_data: extract_request_data(request)
    )
  end

  def start!
    if self.retries_count.nil?
      retries_count = 0
    else
      retries_count = self.retries_count + 1
    end
    update!(status: :running, started_at: Time.now, progress: 0, retries_count: retries_count)
  end

  def error!(error)
    update!(error: error, status: :failed, stopped_at: Time.now)
  end

  def complete!(result=nil)
    result = result.to_json if result

    update!(result: result, status: :completed, progress: 100, stopped_at: Time.now)
  end

  def wrapped_status
    if completed? && result_purged?
      :expired
    else
      status
    end
  end

  def self.extract_request_data(request)
    {
      host: request.host,
      **PaperTrail.request.controller_info
    }
  end

  def generate_presigned_url!
    if result_url.present? && !result_url_expired?
      return result_url
    end

    file_key = self.result['file_key']
    bucket = self.result['bucket']

    return nil unless file_key.present?

    url = S3Service.new.get_presigned_url(bucket, file_key)
    self.result_url = url
    save!

    url
  end

  def result_url_expired?
    return true if result_url_created_at.nil?

    result_url_created_at < AsyncTasks::Manager::RESULT_URL_EXPIRATION.to_i.ago
  end
end
