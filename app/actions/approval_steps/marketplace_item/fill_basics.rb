class ApprovalSteps::MarketplaceItem::FillBasics < ApprovalSteps::BasicStep
  attribute :data_set_id

  def call
    step.result = result
    step.status = outcome
    step.save!
  end

  private

  def result
    {
      data_set_id: data_set_id
    }
  end

  def assignee_scope
    [step.approval_request.requestor]
  end
end
