class Ability
  include CanCan::Ability

  def initialize(user)
    return if user.nil?

    # Define abilities for the passed in user here. For example:
    #
    #   user ||= User.new # guest user (not logged in)
    #   if user.admin?
    #     can :manage, :all
    #   else
    #     can :read, :all
    #   end
    #
    # The first argument to `can` is the action you are giving the user
    # permission to do.
    # If you pass :manage it will apply to every action. Other common actions
    # here are :read, :create, :update and :destroy.
    #
    # The second argument is the resource the user can perform the action on.
    # If you pass :all it will apply to every resource. Otherwise pass a Ruby
    # class of the resource.
    #
    # The third argument is an optional hash of conditions to further filter the
    # objects.
    # For example, here the user can only update published articles.
    #
    #   can :update, Article, :published => true
    #
    # See the wiki for details:
    # https://github.com/CanCanCommunity/cancancan/wiki/Defining-Abilities

    can :manage, FlowNode do |fn|
      fn.has_admin_access?(user)
    end

    can :operate, FlowNode do |fn|
      fn.has_operator_access?(user)
    end

    can :read, FlowNode do |fn|
      fn.public? || fn.has_collaborator_access?(user)
    end

    can :manage, FlowTrigger do |ft|
      ft.has_admin_access?(user)
    end

    can :operate, FlowTrigger do |ft|
      ft.has_operator_access?(user)
    end

    can :read, FlowTrigger do |ft|
      ft.has_collaborator_access?(user)
    end

    can :read, CatalogConfig do |c|
      return (c.owner_id == user.id) if c.org.nil?

      user.org_member?(c.org) || user.super_user?
    end

    can :manage, CatalogConfig do |c|
      (c.org.nil? ? (c.owner_id == user.id) : (c.org.has_admin_access?(user)))
    end

    can :read, GenAiConfig do |c|
      user.org_member?(c.org) || user.super_user?
    end

    can :manage, GenAiConfig do |c|
      c.org.has_admin_access?(user)
    end

    [
      ResourceParameter,
      VendorEndpoint,
      Vendor,
      AuthTemplate,
      AuthParameter
    ].each do |klass|
      can :read, klass do |v|
        user.super_user?
      end

      can :manage, klass do |v|
        user.super_user?
      end
    end

    can :manage, User do |u|
      u.has_admin_access?(user)
    end

    can :operate, User do |u|
      u.has_operator_access?(user)
    end

    can :read, Team do |t|
      t.members.include?(user) || t.has_collaborator_access?(user)
    end

    can :manage, Team do |t|
      t.has_admin_access?(user)
    end

    can :manage, Org do |o|
      o.has_admin_access?(user)
    end

    can :read, Org do |o|
      o.members.include?(user) || o.has_collaborator_access?(user)
    end

    can :operate, Org do |o|
      o.has_operator_access?(user)
    end

    can :manage, OrgCustodian do |org_custodian|
      user.org.has_admin_access?(user) || user.org.custodian?(user)
    end

    can :manage, DataSet do |d|
      d.has_admin_access?(user)
    end

    can :read, DataSet do |d|
      d.public? || d.has_collaborator_access?(user) || d.has_sharer_access?(user, user.org) || user.org.marketplace_data_sets&.include?(d) || user.org.pending_approval_marketplace_data_sets&.include?(d)
    end

    can :transform, DataSet do |d|
      d.public? || d.has_collaborator_access?(user) || d.has_sharer_access?(user, user.org)
    end

    can :operate, DataSet do |d|
      d.has_operator_access?(user)
    end

    can :manage, DataSource do |d|
      d.has_admin_access?(user)
    end

    can :read, DataSource do |d|
      d.has_collaborator_access?(user)
    end

    can :operate, DataSource do |d|
      d.has_operator_access?(user)
    end

    can :manage, DataSchema do |d|
      d.has_admin_access?(user)
    end

    can :read, DataSchema do |d|
      d.public? || d.has_collaborator_access?(user)
    end

    can :read, DataSink do |d|
      d.has_collaborator_access?(user)
    end

    can :manage, DataSink do |d|
      d.has_admin_access?(user)
    end

    can :operate, DataSink do |d|
      d.has_operator_access?(user)
    end

    can :read, DataCredentials do |d|
      d.has_collaborator_access?(user)
    end

    can :manage, DataCredentials do |d|
      d.has_admin_access?(user)
    end

    can :read, DataMap do |d|
      d.public? || d.has_collaborator_access?(user)
    end

    can :manage, DataMap do |d|
      d.has_admin_access?(user)
    end

    can :read, Notification do |n|
      n.has_collaborator_access?(user) || n.resource.try(:has_collaborator_access?, user)
    end

    can :manage, Notification do |n|
      n.has_admin_access?(user) || n.resource.try(:has_admin_access?, user)
    end

    can :read, NotificationChannelSetting do |n|
      n.has_collaborator_access?(user)
    end

    can :manage, NotificationChannelSetting do |n|
      n.has_admin_access?(user)
    end

    can :read, NotificationSetting do |n|
      n.has_collaborator_access?(user)
    end

    can :manage, NotificationSetting do |n|
      n.has_admin_access?(user)
    end

    can :read, QuarantineSetting do |n|
      n.has_collaborator_access?(user)
    end

    can :manage, QuarantineSetting do |n|
      n.has_admin_access?(user)
    end

    can :read, AttributeTransform do |t|
      t.public? || t.has_collaborator_access?(user)
    end

    can :manage, AttributeTransform do |t|
      t.has_admin_access?(user)
    end

    can :read, Transform do |t|
      t.public? || t.has_collaborator_access?(user)
    end

    can :manage, Transform do |t|
      t.has_admin_access?(user)
    end

    can :read, Validator do |v|
      v.public? || v.has_collaborator_access?(user)
    end

    can :manage, Validator do |v|
      v.has_admin_access?(user)
    end

    can :read, CodeContainer do |t|
      t.public? || t.has_collaborator_access?(user)
    end

    can :manage, CodeContainer do |t|
      t.has_admin_access?(user)
    end

    can :read, DocContainer do |t|
      t.public? || t.has_collaborator_access?(user)
    end

    can :manage, DocContainer do |t|
      t.has_admin_access?(user)
    end

    can :read, Project do |t|
      t.has_collaborator_access?(user)
    end

    can :manage, Project do |t|
      t.has_admin_access?(user)
    end

    can :manage, DataFlow do |d|
      d.has_admin_access?(user)
    end

    can :read, DataFlow do |d|
      d.has_collaborator_access?(user)
    end

    can :operate, DataFlow do |d|
      d.has_operator_access?(user)
    end

    can :read, DashboardTransform do |n|
      n.has_collaborator_access?(user)
    end

    can :manage, DashboardTransform do |n|
      n.has_admin_access?(user)
    end

    can :read, CustomDataFlow do |d|
      d.has_collaborator_access?(user)
    end

    can :manage, CustomDataFlow do |d|
      d.has_admin_access?(user)
    end

    can :operate, CustomDataFlow do |d|
      d.has_operator_access?(user)
    end

    can :read, UserSetting do |us|
      ((us.owner_id == user.id) ||
        (!us.org.nil? && us.org.has_admin_access?(user)) ||
        user.super_user?)
    end

    can :manage, UserSetting do |us|
      ((us.owner_id == user.id) ||
        (!us.org.nil? && us.org.has_admin_access?(user)) ||
        user.super_user?)
    end

    can :read, ClusterEndpoint do |ce|
      user.super_user?
    end

    can :manage, ClusterEndpoint do |ce|
      user.super_user?
    end

    can :read, DataSetsCatalogRef do |dcr|
      dcr.catalog_config.org.has_admin_access?(user)
    end

    can :manage, DataSetsCatalogRef do |dcr|
      dcr.catalog_config.org.has_admin_access?(user)
    end

    can :manage, Domain do |domain|
      domain.org&.has_admin_access?(user) || domain.active_custodian_user?(user)
    end

    can :read, Domain do |domain|
      domain.org == user.org
    end

    can :manage, MarketplaceItem do |item|
      item.org.has_admin_access?(user) || item.domains.any? { |domain| domain.active_custodian_user?(user) }
    end

    can :manage, ApprovalStep do |approval_step|
      can? :manage, approval_step.approval_request.topic
    end

    can :manage, ApprovalRequest do |approval_request|
      approval_request.org.has_admin_access?(user) || can?(:manage, approval_request.topic) || user.org_custodian?(approval_request.org)
    end

    can :read, ApprovalRequest do |approval_request|
      can?(:manage, approval_request) || approval_request.requestor == user
    end

    can :read, AsyncTask do |async_task|
      (async_task.owner_id == user.id) || async_task.org&.has_collaborator_access?(user)
    end

    can :manage, AsyncTask do |async_task|
      (async_task.owner_id == user.id) || async_task.org&.has_admin_access?(user)
    end

    can :read, Runtime do |runtime|
      user.org_member?(runtime.org)
    end

    can :manage, Runtime do |runtime|
      runtime.org.has_admin_access?(user)
    end

    can :read, DataCredentialsGroup do |group|
      group.has_collaborator_access?(user)
    end

    can :manage, DataCredentialsGroup do |group|
      group.has_admin_access?(user)
    end
  end
end
