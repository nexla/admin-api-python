class ApprovalSteps::Reject < BaseAction
  attribute :reason
  attribute :step

  delegate :approval_request, to: :step

  def call
    step.update!(status: :rejected, result: {reason: reason})
    approval_request.update!(status: :rejected)
  end
end
