module AsyncTasks::Tasks
  class ChownUserResources < AsyncTasks::Tasks::Base
    extend Memoist

    def run
      owner = User.find(args[:from_user_id])
      org = args[:from_org_id].present? ?  Org.find(args[:from_org_id]) : nil
      owner.org = org || task.org

      delegate_owner = User.find(args[:to_user_id])
      delegate_org = Org.find(args[:to_org_id]) if args[:to_org_id].present?

      TransferUserResources.transfer(owner, org, delegate_owner, delegate_org) do |count, total|
        report_progress(count, total)
      end
    end

    def check_preconditions
      unless args[:from_user_id].present?
        raise Api::V1::ApiError.new(:bad_request, "Required owner_id missing from input")
      end
      from_user = User.find(args[:from_user_id])

      ability = Ability.new(task_owner)
      raise Api::V1::ApiError.new(:forbidden) unless ability.can?(:manage, from_user)

      unless args[:to_user_id].present?
        raise Api::V1::ApiError.new(:bad_request, "Required delegate_owner_id missing from input")
      end
      delegate_owner = User.find(args[:to_user_id])

      unless args.key?(:from_org_id)
        raise Api::V1::ApiError.new(:bad_request, "Required org_id missing from input")
      end

      org = args[:from_org_id].blank? ? nil : Org.find(args[:from_org_id])

      delegate_org = args[:to_org_id].blank? ? nil : Org.find(args[:to_org_id])
      raise Api::V1::ApiError.new(:forbidden) if delegate_org.present? && !task.owner.super_user?

      if org.present?
        if delegate_org.present?
          raise Api::V1::ApiError.new(:forbidden) unless delegate_owner.org_member?(delegate_org)
        else
          raise Api::V1::ApiError.new(:forbidden) unless delegate_owner.org_member?(org)
        end
      else
        if !task.owner.super_user? && (task.owner.id != from_user.id)
          raise Api::V1::ApiError.new(:forbidden)
        end
      end
    end

    def explain_arguments
      {
        from_user_id: "ID of the current owner user (optional). If not provided, the task owner will be used.",
        from_org_id: "ID of the organization whose resources will be chowned (optional). If not provided, the task org context will be used.",
        to_user_id: "ID of the new owner user (required).",
        to_org_id: "ID of the organization whose resources will be chowned (optional)."
      }
    end

  end
end