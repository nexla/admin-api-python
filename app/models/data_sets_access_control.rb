class DataSetsAccessControl < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include AccessControlUtils

  after_create do
    self.handle_update(false)
  end

  before_destroy :handle_before_destroy
  
  belongs_to :data_set, required: true
  belongs_to :accessor_org, class_name: "Org", foreign_key: "accessor_org_id"

  def render
    case self.accessor_type
    when AccessControls::Accessor_Types[:user]
      u = User.find_by_id(self.accessor_id)
      if u.nil?
        Rails.logger.warn("User #{self.accessor_id} not found for ACL #{self.id}")
        return nil
      end

      return {
        :id => u.id,
        :email => u.email,
        :org_id => self.accessor_org_id,
        :notified_at => self.notified_at,
        :name => self.name,
        :description => self.description
      }
    when AccessControls::Accessor_Types[:org]
      return {
        :org_id => self.accessor_id,
        :name => self.name,
        :description => self.description
      }
    when AccessControls::Accessor_Types[:team]
      return {
        :team_id => self.accessor_id,
        :org_id => self.accessor_org_id,
        :name => self.name,
        :description => self.description
      }
    end
    return nil
  end

  protected

  def handle_before_destroy
    self.handle_update(true)
  end

  def handle_update (is_destroy)
    # The cases:
    # 1. Creating and/or activating the ACL record
    # 2. Destroying or pausing the ACL record
    # 3. Updating the ACL record to include "sharer" role
    # 4. Updating the ACL record to remove the "sharer" role

    sharing_enabled = !is_destroy && DataSet.access_roles_enable_role?(self.role_index, :sharer)

    self.dependent_data_sets.each do |ds|
      # Note, we don't send control messages for downstream
      # flows that are not activated in the owner/user's workflow.
      next if !ds.active?
      ControlService.new(ds).publish(sharing_enabled ? :activate : :pause)
    end
  end

  def dependent_data_sets
    # Data sets that depend on the shared data set through this ACL instance have:
    # 1. The shared data set's id in 'parent_data_set_id'
    # 2. Sharing enabled by this ACL instance and no other rule (e.g. org default access role, etc.)
    
    dependent = []
    data_sets = DataSet.where(parent_data_set_id: self.data_set_id)

    data_sets.each do |ds|
      dependent << ds if self.sharing_access_enabled_by(ds)
    end

    return dependent
  end

  def sharing_access_enabled_by (data_set)
    # Return true if the incoming data set has access to its parent data set
    # enabled by this ACL instance and no other rule, such as the default Org access role.

    # Cases
    # 1. Accessor type is USER and data set owner_id == accessor_id
    # 2. Accessor type is TEAM and data set owner_id is member of accessor_id TEAM (and USER has no overriding ACL)
    # 3. Accessor type is ORG and data set org_id == accessor_id (and USER/TEAM has no overriding ACL)

    # First, check for default access...
    if (!data_set.org_id.nil? && (data_set.org_id == self.data_set.org_id))
      # Parent and child data sets are owned within the same Org. If owner of child is an
      # Admin, or the Org has Admin access by default, this ACL instance does not control
      # the child data set's access to the parent.
      data_set.owner.org = data_set.org
      return false if data_set.org.has_admin_access?(data_set.owner)
    end

    # Then check if the current ACL enables sharing...
    return false if !DataSet.access_roles_enable_role?(self.role_index, :sharer)
    return false if (data_set.org_id != self.accessor_org_id)

    # And, finally, check if the current ACL applies to the accessor...
    enabled = false

    case self.accessor_type
      when AccessControls::Accessor_Types[:user]
        enabled = (data_set.owner_id == self.accessor_id)
      when AccessControls::Accessor_Types[:team]
        team = Team.find(self.accessor_id)
        enabled = (!team.nil? && !data_set.owner.nil? && data_set.owner.team_member?(team))
      when AccessControls::Accessor_Types[:org]
        org = Org.find(self.accessor_id)
        enabled = (!org.nil? && !data_set.owner.nil? && data_set.owner.org_member?(org))
    end

    return enabled
  end

end
