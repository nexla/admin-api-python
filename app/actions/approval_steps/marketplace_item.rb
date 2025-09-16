module ApprovalSteps::MarketplaceItem
  def self.steps
    [FillBasics, ApproveRequest]
  end
end
