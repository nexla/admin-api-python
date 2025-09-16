class FlowTrigger < ApplicationRecord
  include Api::V1::Schema
  include AuditLog

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :triggered_origin_node, class_name: "FlowNode", foreign_key: "triggered_origin_node_id"
  belongs_to :triggering_flow_node, class_name: "FlowNode", foreign_key: "triggering_flow_node_id"
  belongs_to :triggering_origin_node, class_name: "FlowNode", foreign_key: "triggering_origin_node_id"
  belongs_to :triggering_event, class_name: "OrchestrationEventType", 
    foreign_key: "triggering_event_type", primary_key: "type"
  belongs_to :triggered_event, class_name: "OrchestrationEventType", 
    foreign_key: "triggered_event_type", primary_key: "type"

  before_save :ensure_triggering_origin
  after_commit :handle_after_commit, on: [:create, :update, :destroy]

  STATUSES = {
    active: "ACTIVE",
    paused: "PAUSED"
  }

  RESOURCE_TYPES = {
    data_source: DataSource, 
    data_sink: DataSink
  }

  attr_accessor :control_messages_enabled

  after_initialize do
    self.control_messages_enabled = true
  end

  def self.validate_resource_type (resource_type_str = "unknown")
    RESOURCE_TYPES[resource_type_str.downcase.to_sym]
  end

  def self.accessible_by_user (user, org, input_opts = {})
    # We redefine accessible_by_user here because FlowTrigger access is
    # controled by FlowNode access. There's no FlowTriggersAccessControl
    # model.
    fn_ids = FlowNode.accessible_by_user(user, org, { access_role: :admin }).pluck(:id)
    FlowTrigger.where(triggered_origin_node_id: fn_ids).or(FlowTrigger.where(triggering_flow_node_id: fn_ids))
  end

  def self.build_from_input (api_user_info, input)
    return nil if (!input.is_a?(Hash) || api_user_info.nil?)
    input.symbolize_keys!

    if input[:triggered_origin_node_id].blank? && 
      input[:triggered_resource_id].blank? &&
      input[:data_source_id].blank?
      raise Api::V1::ApiError.new(:bad_request, 
          "triggered_origin_node_id or triggered_resource_id is required")
    end

    if input[:triggering_flow_node_id].blank? &&
      input[:triggering_resource_id].blank? &&
      input[:data_sink_id].blank? && input[:data_source_id].blank?
      raise Api::V1::ApiError.new(:bad_request, 
          "triggering_flow_node_id or triggering_resource_id is required")
    end

    ft = FlowTrigger.where(triggered_origin_node_id: input[:triggered_origin_node_id],
      triggering_flow_node_id: input[:triggering_flow_node_id]).first
    if ft.present?
      raise Api::V1::ApiError.new(:conflict, 
        "A flow trigger already exists for triggered and triggering resources")
    end

    FlowTrigger.new.update_mutable!(api_user_info, input)
  end

  def update_mutable! (api_user_info, input)
    ability = Ability.new(api_user_info.input_owner)

    FlowTrigger.transaction do
      self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
      self.org = api_user_info.input_org if (self.org != api_user_info.input_org)
      self.triggering_event_type = input[:triggering_event_type] if input.key?(:triggering_event_type)
      self.triggered_event_type = input[:triggered_event_type] if input.key?(:triggered_event_type)

      if input.key?(:triggered_origin_node_id) || 
        input.key?(:triggered_resource_id) ||
        input.key?(:data_source_id)
        self.triggered_origin_node = self.triggered_origin_node_from_input(input, ability)
      end

      if input.key?(:triggering_flow_node_id) ||
        input.key?(:triggering_resource_id) ||
        input.key?(:data_sink_id) ||
        input.key?(:data_source_id)
        self.triggering_flow_node = self.triggering_flow_node_from_input(input, ability)
        self.triggering_origin_node = self.triggering_flow_node.origin_node
      end

      if (self.triggering_flow_node.origin_node_id == self.triggered_origin_node_id)
        raise Api::V1::ApiError.new(:bad_request, "A flow node cannot trigger its own origin node")
      end

      self.save!

      loop = Triggers::CycleDetector.new(self.triggered_origin_node).detect
      if loop.present?
        other = loop.reject{|t| t.id == self.id }
        msg = "A trigger will create a cycle in flows. Other triggers in the chain: #{other.pluck(:id).join(", ")}"
        raise Api::V1::ApiError.new(:bad_request, msg, nil, loop)
      end
    end

    self
  end

  def triggered_resource
    self.triggered_origin_node&.resource
  end

  def triggering_resource
    self.triggering_flow_node&.resource
  end

  def active?
    (self.status == FlowTrigger::STATUSES[:active])
  end

  def activate!
    self.transaction do
      if !self.active?
        self.status = FlowTrigger::STATUSES[:active]
        self.save!
      end
    end
  end

  def paused?
    (self.status == FlowTrigger::STATUSES[:paused])
  end

  def pause!
    self.transaction do
      if !self.paused?
        self.status = FlowTrigger::STATUSES[:paused]
        self.save!
      end
    end
  end

  # Note, we aren't including the AccessControls module in this model
  # because all access to a flow_trigger is determined by access to its
  # triggered and triggering resources.

  def has_admin_access? (user)
    return false if (!self.triggered_origin_node.present? || !self.triggering_flow_node.present?)
    return false if !self.triggered_origin_node.has_admin_access?(user)
    self.triggering_flow_node.has_admin_access?(user)
  end

  def has_operator_access? (user)
    return false if (!self.triggered_origin_node.present? || !self.triggering_flow_node.present?)
    return false if !self.triggered_origin_node.has_operator_access?(user)
    self.triggering_flow_node.has_operator_access?(user)
  end

  def has_collaborator_access? (user)
    return false if (!self.triggered_origin_node.present? || !self.triggering_flow_node.present?)
    return false if !self.triggered_origin_node.has_collaborator_access?(user)
    self.triggering_flow_node.has_collaborator_access?(user)
  end

  protected

  def triggered_origin_node_from_input (input, ability)
    triggered_origin_node = nil

    if input[:triggered_origin_node_id].present?
      triggered_origin_node = FlowNode.find(input[:triggered_origin_node_id])
    elsif input[:data_source_id].present?
      triggered_origin_node = DataSource.find(input[:data_source_id]).origin_node
    else
      resource_model = FlowTrigger.validate_resource_type(input[:triggered_resource_type])
      raise Api::V1::ApiError.new(:bad_request,
        "A valid triggered_resource_type is required") if resource_model.nil?
      triggered_origin_node = resource_model
        .find(input[:triggered_resource_id]).origin_node
    end

    if (!ability.can?(:manage, triggered_origin_node))
      raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to triggered resource")
    end

    triggered_origin_node
  end

  def triggering_flow_node_from_input (input, ability)
    triggering_flow_node = nil

    if input[:triggering_flow_node_id].present?
      triggering_flow_node = FlowNode.find(input[:triggering_flow_node_id])
    elsif input[:data_sink_id].present?
      triggering_flow_node = DataSink.find(input[:data_sink_id]).flow_node
    elsif input[:data_source_id].present?
      triggering_flow_node = DataSource.find(input[:data_source_id]).origin_node
    else
      resource_model = FlowTrigger.validate_resource_type(input[:triggering_resource_type])
      raise Api::V1::ApiError.new(:bad_request,
        "A valid triggering_resource_type is required") if resource_model.nil?
      triggering_flow_node = resource_model
        .find(input[:triggering_resource_id]).flow_node
    end

    if (!ability.can?(:manage, triggering_flow_node))
      raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to triggered resource")
    end

    triggering_flow_node
  end

  def handle_after_commit

    if self.triggered_resource.respond_to?(:send_control_event)
       self.triggered_resource.send_control_event(:update)
    end
    if self.triggering_resource.respond_to?(:send_control_event)
      self.triggering_resource.send_control_event(:update)
    end
  end

  def ensure_triggering_origin
    if self.triggering_origin_node_id.blank? && self.triggering_flow_node_id.present?
      self.triggering_origin_node_id = self.triggering_flow_node.origin_node_id
    end
  end
end
