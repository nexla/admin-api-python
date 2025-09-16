class FlowLink < ActiveRecord::Base
  include Api::V1::Schema
  include AuditLog

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :left_origin_node, class_name: "FlowNode", foreign_key: "left_origin_node_id"
  belongs_to :right_origin_node, class_name: "FlowNode", foreign_key: "right_origin_node_id"
  belongs_to :data_set, class_name: "DataSet", foreign_key: 'retriever_data_set_id', optional: true

  MAX_LINK_DEPTH = 5

  def self.all_linked_origin_node_ids (origin_node_ids)
    count = 0
    origin_node_ids = origin_node_ids.uniq.sort

    while (count < MAX_LINK_DEPTH) do
      ids = FlowLink.where(left_origin_node_id: origin_node_ids)
        .or(FlowLink.where(right_origin_node_id: origin_node_ids))
        .pluck(:left_origin_node_id, :right_origin_node_id)
        .flatten.uniq.sort
      break if ids.empty?
      break if (ids == origin_node_ids)
      origin_node_ids = ids
      count += 1
    end

    origin_node_ids
  end

  def self.build_from_input (api_user_info, input)
    return nil if (!input.is_a?(Hash) || api_user_info.nil?)
    input.symbolize_keys!

    in_l = input[:left_origin_node_id]
    in_r = input[:right_origin_node_id]
    in_ds = input[:retriever_data_set_id]
    in_type = (input[:link_type] || FlowLinkType.types(:linked_flow))

    if in_l.nil? || in_r.nil?
      raise Api::V1::ApiError.new(:bad_request, "Left and right origin_node_id are both required")
    end

    fl = FlowLink.where(left_origin_node_id: [in_l, in_r],
      right_origin_node_id: [in_l, in_r], link_type: in_type, retriever_data_set_id: in_ds)

    if fl.present?
      raise Api::V1::ApiError.new(:conflict,
        "A flow link of type #{in_type} already exists for these flows: [#{in_l}, #{in_r}]#{" and data retriever [#{in_ds}]" if in_ds.present? }")
    end

    input[:link_type] = in_type
    FlowLink.new.update_mutable!(api_user_info, input)
  end

  def update_mutable! (api_user_info, input)
    ability = Ability.new(api_user_info.input_owner)

    FlowLink.transaction do
      self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
      self.org = api_user_info.input_org if (self.org != api_user_info.input_org)
      self.link_type = input[:link_type] if input.key?(:link_type)
      self.retriever_data_set_id = input[:retriever_data_set_id] if input.key?(:retriever_data_set_id)

      # We allow passing any flow_node_id from the linked flows,
      # which we promote to the origin_node ids.

      if input.key?(:left_origin_node_id)
        on = FlowNode.origin_node(input[:left_origin_node_id])
        if !ability.can?(:manage, on)
          raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to flow: #{on.id}")
        end
        self.left_origin_node_id = on.id
      end

      if input.key?(:right_origin_node_id)
        on = FlowNode.origin_node(input[:right_origin_node_id])
        if !ability.can?(:manage, on)
          raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to flow: #{on.id}")
        end
        self.right_origin_node_id = on.id
      end

      if (self.left_origin_node_id == self.right_origin_node_id)
        # No circular linking check beyond the reflexive one.
        # If you want to create a circle of linked flows, go for it!
        raise Api::V1::ApiError.new(:bad_request, "A flow cannot be linked to itself")
      end

      if (self.link_type == FlowLinkType.types[:linked_flow])
        # By convention, we always put the smaller origin_node_id
        # on the left when the link type is bi-directional.
        self.left_origin_node_id, self.right_origin_node_id =
          [self.left_origin_node_id, self.right_origin_node_id].sort
      end

      self.save!
    end

    self
  end

  # We aren't including the AccessControls or Accessible modules
  # in this model because all access to a flow_link is determined by
  # access to its origin nodes.

  def self.accessible_by_user (user, org, input_opts = {})
    fn_ids = FlowNode.accessible_by_user(user, org, { access_role: :admin }).pluck(:id)
    FlowLink.where(left_origin_node_id: fn_ids).or(FlowTrigger.where(right_origin_node_id: fn_ids))
  end

  def has_admin_access? (user)
    return false if (!self.left_origin_node.present? || !self.right_origin_node.present?)
    return false if !self.left_origin_node.has_admin_access?(user)
    self.right_origin_node.has_admin_access?(user)
  end

  def has_operator_access? (user)
    return false if (!self.left_origin_node.present? || !self.right_origin_node.present?)
    return false if !self.left_origin_node.has_operator_access?(user)
    self.right_origin_node.has_operator_access?(user)
  end

  def has_collaborator_access? (user)
    return false if (!self.left_origin_node.present? || !self.right_origin_node.present?)
    return false if !self.left_origin_node.has_collaborator_access?(user)
    self.right_origin_node.has_collaborator_access?(user)
  end
end
