class ApprovalStep < ApplicationRecord
  belongs_to :approval_request
  belongs_to :assignee, class_name: 'User'

  delegate :org, to: :approval_request

  enum status: {pending: 'pending', approved: 'approved', rejected: 'rejected'}

  serialize :result

  DEFAULT_STATUS = 'pending'

  def action_class
    "#{approval_request.action_namespace}::#{step_name.camelize}".constantize
  end
end
