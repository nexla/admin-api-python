class ApprovalSteps::MarketplaceItemAccess::RequestAccess < ApprovalSteps::BasicStep
  validate :performer_validation

  def call
    step.status = outcome
    step.save!

    DataSets::Share.new(performer: performer, org: org, data_set: data_set, user: approval_request.requestor).perform!
  end

  private

  delegate :data_set, to: :topic
  delegate :domains, to: :topic

  def assignee_scope
    approval_request.topic.domain_custodian_users
  end

  def performer_validation
    errors.add(:performer, :unauthorised) unless domains.any? { |domain| Ability.new(performer).can?(:manager, domain) }
  end
end
