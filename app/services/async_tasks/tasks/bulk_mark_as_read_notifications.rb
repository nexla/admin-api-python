module AsyncTasks::Tasks
  class BulkMarkAsReadNotifications < AsyncTasks::Tasks::Base
    def run
      scope = Notification.where(owner: owner)
      if args[:period_start].present?
        scope = scope.where("created_at >= ?", Date.parse(args[:period_start]))
      end
      if args[:period_end].present?
        scope = scope.where("created_at <= ?", Date.parse(args[:period_end]))
      end

      total = scope.count
      processed = 0
      report_progress(0, total)

      scope.in_batches do |relation|
        relation.update_all(read_at: Time.current)
        processed += relation.size
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

      if args[:period_start].present?
        begin
          Date.parse(args[:period_start])
        rescue ArgumentError
          raise Api::V1::ApiError.new(:bad_request, "Invalid period_start date")
        end
      end

      if args[:period_end].present?
        begin
          Date.parse(args[:period_end])
        rescue ArgumentError
          raise Api::V1::ApiError.new(:bad_request, "Invalid period_end date")
        end
      end
    end

    def explain_arguments
      {
        owner_id: "ID of the user whose notifications will be deleted (optional). If not provided, the task initiator's notifications will be deleted.",
        period_start: "Start of the period for which notifications will be deleted (optional). If not provided, notifications from beginning of time will be deleted.",
        period_end: "End of the period for which notifications will be deleted (optional). If not provided, notifications up to current time will be deleted."
      }
    end

    private

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