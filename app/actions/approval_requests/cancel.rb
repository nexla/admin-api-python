class ApprovalRequests::Cancel < BaseAction
  attribute :approval_request

  validate :ensure_ownership
  validate :ensure_pending

  def call
    approval_request.destroy!
  end

  private

  def ensure_ownership
    errors.add(:performer, 'is not an owner') if approval_request.requestor != performer
  end

  def ensure_pending
    errors.add(:approval_request, 'is not in pending state') unless approval_request.pending?
  end
end
