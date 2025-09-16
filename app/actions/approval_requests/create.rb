class ApprovalRequests::Create < BaseAction
  attribute :type
  attribute :topic

  # TODO: This is a temporary patch until I solve the problem in more elegant way.
  attribute :unique, optional: true

  validates :type, inclusion: {in: ::ApprovalRequest.request_types}

  def call
    approval_request.save!
    step = create_initial_step

    approve_step(step)

    approval_request.last_step
  end

  private

  def create_initial_step
    approval_request.approval_steps.find_or_create_by!(step_name: "ApprovalSteps::#{type.camelcase}".constantize.steps.first.step_name)
  end

  def approve_step(step)
    action = step.action_class.new(performer: performer, org: org, step: step, outcome: "Auto approval")
    return unless action.valid?

    action.perform!
  end

  memoize def approval_request
    if unique
      ApprovalRequest.find_or_initialize_by(requestor: performer, org: org, request_type: type, topic: topic, status: :pending)
    else
      ApprovalRequest.new(requestor: performer, org: org, request_type: type, topic: topic, status: :pending)
    end
  end
end
