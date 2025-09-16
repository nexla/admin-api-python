module FlowTriggersConcern
  extend ActiveSupport::Concern

  def flow_triggers
    FlowTrigger.where(triggered_origin_node_id: self.origin_node_id)
       .or(FlowTrigger.where(triggering_flow_node_id: self.flow_node_id))
  end

  def update_flow_triggers (api_user_info, flow_triggers, mode, all = false)
    return if flow_triggers.empty? && !all

    case mode
    when :add
      self.add_flow_triggers(api_user_info, flow_triggers)
    when :reset
      # Should we require triggers to be paused first?
      FlowTrigger.where(triggered_origin_node_id: self.flow_node_id).destroy_all
      self.add_flow_triggers(api_user_info, flow_triggers)
    when :remove
      self.remove_flow_triggers(api_user_info, flow_triggers, all)
    when :activate, :pause
      self.activate_flow_triggers(api_user_info, flow_triggers, mode, all)
    end
  end

  def add_flow_triggers (api_user_info, flow_triggers)
    validated_triggers = []
    flow_triggers.each do |flow_trigger|
      validated_triggers << validate_flow_trigger(flow_trigger)
    end
    self.transaction do
      validated_triggers.each do |flow_trigger|
        FlowTrigger.build_from_input(api_user_info, flow_trigger)
      end
    end
  end

  def remove_flow_triggers (api_user_info, flow_triggers, all = false)
    flow_triggers = self.flow_triggers if all
    flow_triggers.each do |flow_trigger|
      ft = nil
      if flow_trigger.is_a?(Integer)
        ft = FlowTrigger.find_by(id: flow_trigger)
        if !ft.present?
          raise Api::V1::ApiError.new(:not_found, "Flow trigger not found: #{flow_trigger}")
        end
        if !ft.has_admin_access?(api_user_info.user)
          raise Api::V1::ApiError.new(:forbidden, "Invalid access to flow trigger")
        end
      elsif flow_trigger.is_a?(FlowTrigger)
        ft = flow_trigger
      else
      end

      if !ft.has_admin_access?(api_user_info.user)
        raise Api::V1::ApiError.new(:forbidden, "Invalid access to flow trigger")
      end

      ft.destroy if ft.present?
    end
  end

  def activate_flow_triggers (api_user_info, flow_triggers, mode, all = false)
    flow_triggers = self.flow_triggers if all
    flow_triggers.each do |flow_trigger|
      ft = nil
      if flow_trigger.is_a?(Integer)
        ft = FlowTrigger.find_by(id: flow_trigger)
      elsif flow_trigger.is_a?(FlowTrigger)
        ft = flow_trigger
      else
      end
      next if !ft.present?

      if !ft.has_operator_access?(api_user_info.user)
        raise Api::V1::ApiError.new(:forbidden, "Invalid access to flow trigger")
      end

      ft.transaction do
        if (mode == :activate)
          ft.activate!
        else
          ft.pause!
        end
      end
    end
  end

  protected

  def validate_flow_trigger (flow_trigger)
    if self.is_a?(DataSink)
      case flow_trigger[:triggered_event_type]
      when OrchestrationEventType::Types[:data_source_read_start]
        flow_trigger[:triggering_flow_node_id] = self.flow_node_id
        flow_trigger[:triggering_event_type] = OrchestrationEventType::Types[:data_sink_write_done]
      else
        raise Api::V1::ApiError.new(:bad_request, 
          "Unsupported triggered event type: #{flow_trigger[:triggered_event_type]}")
      end
    elsif self.is_a?(DataSource)
      case flow_trigger[:triggering_event_type]
      when OrchestrationEventType::Types[:data_sink_write_done]
        flow_trigger[:triggered_origin_node_id] = self.origin_node_id
        flow_trigger[:triggered_event_type] = OrchestrationEventType::Types[:data_source_read_start]
      when OrchestrationEventType::Types[:data_source_read_done]
        flow_trigger[:triggered_origin_node_id] = self.flow_node_id
        flow_trigger[:triggered_event_type] = OrchestrationEventType::Types[:data_source_read_start]
      else
        raise Api::V1::ApiError.new(:bad_request, 
          "Unsupported triggered event type: #{flow_trigger[:triggering_event_type]}")
      end
    else
      raise Api::V1::ApiError.new(:bad_request, 
        "Unsupported flow orchestration resource: #{self.class.name.underscore}")
    end

    flow_trigger
  end

end
