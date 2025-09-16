module AsyncTasks::Tasks
  class DeactivateUser < BulkPauseFlows
    def run
      super

      # To pause user's resources completely, we have to iterate orgs.
      OrgMembership.where(user_id: target_user.id, status: OrgMembership::Statuses[:active]).each do |org_membership|
        target_user.deactivate!(org_membership.org, true)
      end
      target_user.update(status: User::Statuses[:deactivated])
    end

    def check_preconditions
      raise Api::V1::ApiError.new(:forbidden, "You don't have admin access to target user") unless target_user.has_admin_access?(task.owner)
    end

    def explain_arguments
      {
        user_id: "ID of the user who will be deactivated (required). You need admin access to this user"
      }
    end

    memoize
    def target_user
      User.find(args[:user_id]).tap do |user|
        user.org = task.org
      end
    end
  end
end