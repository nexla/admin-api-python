module AsyncTasks::Tasks
  class CallProbe < AsyncTasks::Tasks::Base
    extend Memoist

    ALLOWED_ACTIONS = %w(authenticate read_sample).freeze
    ALLOWED_ENTITY_TYPES = %w(DataCredentials DataSet DataSink DataSource DataMap).freeze

    ACTIONS_WITH_INPUT = %w(read_sample read_quarantine_sample list_files list_tree detect_schemas).freeze

    def run
      action = args[:action] || 'authenticate'
      if action == 'read_sample'
        result = ProbeService.new(entity).send(action, args[:input].to_h, nil, request_wrapper, task.owner)
      elsif action == 'read_quarantine_sample'
        result = ProbeService.new(entity).send(action, args[:input].to_h)
      else
        result = ProbeService.new(entity).send(action)
      end
      if entity.is_a?(DataCredentials)
        entity.verified_status = "#{result[:status]}" + " #{result[:message]}"
        entity.save!
      end
      result
    end

    def check_preconditions
      unless args[:type].present?
        raise Api::V1::ApiError.new(:bad_request, "Resource type for probe is required")
      end

      unless ALLOWED_ACTIONS.include?(action)
        raise Api::V1::ApiError.new(:bad_request, "Invalid action. Allowed actions: #{ALLOWED_ACTIONS.join(', ')}")
      end

      unless ALLOWED_ENTITY_TYPES.include?(args[:type])
        raise Api::V1::ApiError.new(:bad_request, "Invalid type. Allowed types: #{ALLOWED_ENTITY_TYPES.join(', ')}")
      end

      raise Api::V1::ApiError.new(:bad_request, "ID is required") unless args[:id].present?
      raise Api::V1::ApiError.new(:not_found, "Entity not found") unless entity
      raise Api::V1::ApiError.new(:forbidden, "No access to requested entity") unless entity.has_collaborator_access?(task_owner)

      if action == 'read_sample'
        raise Api::V1::ApiError.new(:bad_request, "`input` required for read_sample") unless args[:input].present?
      end
    end

    def explain_arguments
      {
        type: "Type of the entity to be probed (required). Possible options: #{ALLOWED_ENTITY_TYPES.join(', ')}.",
        id: "ID of the entity to be called (required).",
        action: "Action to be performed on the entity (optional). Possible options: authenticate, read_sample. Default: authenticate.",
        input: "Input to be used for read_sample action (optional)."
      }
    end

    private
    memoize
    def entity
      type = args[:type]
      id = args[:id]
      klass = type.constantize
      klass.find_by(id: id)
    end

    def action
      args[:action] || 'authenticate'
    end

  end
end