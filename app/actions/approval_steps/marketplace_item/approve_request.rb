class ApprovalSteps::MarketplaceItem::ApproveRequest < ApprovalSteps::BasicStep
  validate :performer_validation

  def call
    step.status = :approved
    step.save!

    approve_marketplace_item
  end

  def approve_marketplace_item
    approval_request.topic.marketplace_items.create!(**item_attributes)
  end

  def domain
    topic = approval_request.topic
    topic.is_a?(Domain) ? topic : nil
  end

  def item_attributes
    {
      status: :active,
      **fill_basics_step.result
    }
  end

  def fill_basics_step
    approval_request.approval_steps.first
  end

  def assignee_scope
    result = step.approval_request.org.org_custodian_users
    result += domain.domain_custodian_users if domain
    result
  end

  def performer_validation
    errors.add(:performer, :unauthorised) unless Ability.new(performer).can?(:manager, topic)
  end
end
