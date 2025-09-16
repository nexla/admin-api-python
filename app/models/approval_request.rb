class ApprovalRequest < ApplicationRecord
  include JsonAccessor
  include Api::V1::Schema

  belongs_to :org
  belongs_to :requestor, class_name: 'User'
  belongs_to :topic, polymorphic: true

  has_many :approval_steps, dependent: :destroy

  enum request_type: { marketplace_item: 'marketplace_item', marketplace_item_access: 'marketplace_item_access' }
  enum status: { pending: 'pending', approved: 'approved', rejected: 'rejected' }

  DEFAULT_STATUS = 'pending'

  def current_step
    approval_steps.pending.first
  end

  def last_step
    approval_steps.last
  end

  def first_step
    approval_steps.first
  end

  def action_namespace
    "ApprovalSteps::#{request_type.camelize}".constantize
  end

  def rejection_reason
    last_step = approval_steps.last
    return unless last_step.present?
    last_step.result[:reason]
  end
end
