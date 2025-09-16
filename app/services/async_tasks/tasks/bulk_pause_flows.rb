module AsyncTasks::Tasks
  class BulkPauseFlows < AsyncTasks::Tasks::Base
    def run
      flows = FlowNode.where(owner: owner).origins_only
      if args[:id].present? && args[:id] != 'all'
        flows = flows.where(id: args[:id])
      end
      total = flows.count
      processed = 0

      report_progress(0, total)
      flows.find_each do |flow|
        flow.flow_pause!(all: true)
        processed += 1
        report_progress(processed, total)
      end
      total
    end

    def check_preconditions
      if args[:owner_id].present?
        target_user = User.find_by(id: args[:owner_id])
        raise Api::V1::ApiError.new(:not_found, "User #{args[:owner_id]} does not exist") unless target_user
        raise Api::V1::ApiError.new(:forbidden, "You don't have admin access to target user") unless target_user.has_admin_access?(task_owner)
      end
    end

    def explain_arguments
      {
        owner_id: "ID of the user whose flows will be paused. If not provided, the task owner's flows will be paused.",
        id: "ID of the flow to pause, or 'all' to pause all flows. By default all flows are paused."
      }
    end

    def owner
      if args[:owner_id].present?
        return User.find(args[:owner_id]).tap do |user|
          user.org = task.org
        end
      end
      task_owner
    end
  end
end