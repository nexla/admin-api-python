class ApprovalSteps::BasicStep < BaseAction
  attribute :outcome
  attribute :step

  delegate :approval_request, to: :step
  delegate :topic, to: :approval_request

  def self.step_name
    self.to_s.split('::').last
  end

  private

  before_validation do
    step.assignee_id = performer.id
  end

  after_commit do
    if next_step
      step = approval_request.approval_steps.create!(step_name: next_step.step_name)

      action = step.action_class.new(performer: performer, org: org, step: step, outcome: "Auto approval")
      action.perform

      if action.valid?
        action.perform! # Trigger chain of approval
      else
        action.assignee_scope.each do |assignee|
          Notification.create!(owner: assignee, org: approval_request.org, level: :info, resource_id: step.id, resource_type: 'APPROVAL_STEP', message: 'Approval Request is waiting for your action.')
        end
      end
    else
      approval_request.status = approval_request.approval_steps.last.status if approval_request.approval_steps.any?
      approval_request.save!
    end
  end

  # Define who could be assigned to perform step
  def assignee_scope
    raise NotImplementedError
  end

  def next_step
    self.class.next_step
  end

  def previous_step
    self.class.previous_step
  end

  class << self
    def parent_class
      self.to_s.split('::')[0..-2].join('::').constantize
    end

    def next_step
      steps = parent_class.steps
      steps[steps.index(self) + 1]
    end

    def previous_step
      steps = parent_class.steps
      idx = steps.index(self)

      return nil if idx.zero?

      steps[idx - 1]
    end
  end
end
