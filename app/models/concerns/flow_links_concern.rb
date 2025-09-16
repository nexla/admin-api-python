module FlowLinksConcern
  extend ActiveSupport::Concern

  def flow_links
    # This module is included in FlowNode, but may also be included in
    # DataSource at some point, so we have to get the origin_node_id
    # from the current instance (because flow links are always between
    # origin nodes).
    origin_node_id = self.origin_node_id
    FlowLink.where(left_origin_node_id: origin_node_id)
      .or(FlowLink.where(right_origin_node_id: origin_node_id))
      .where(link_type: FlowLinkType.types[:linked_flow])
  end

  def linked_flow_ids
    origin_node_id = self.origin_node_id
    self.flow_links.map do |fl|
      (origin_node_id == fl.left_origin_node_id) ?
        fl.right_origin_node_id : fl.left_origin_node_id
    end.uniq.compact
  end

  def linked_rag_flow_ids
    FlowNode.where(id: linked_flow_ids, flow_type: FlowNode::Flow_Types[:rag]).ids
  end

  def update_linked_flows (api_user_info, input, mode, all = false)
    origin_node = self.origin_node
    return if origin_node.nil?

    case mode
    when :add
      origin_node.add_linked_flows(api_user_info, input[:linked_flows], input[:retriever_data_sets])
    when :reset
      origin_node.flow_links.destroy_all
      origin_node.add_linked_flows(api_user_info, input[:linked_flows], input[:retriever_data_sets])
    when :remove
      origin_node.remove_linked_flows(api_user_info, input[:linked_flows], input[:retriever_data_sets], all)
    end
  end

  def add_linked_flows (api_user_info, linked_flows, retriever_data_sets = nil)
    self.transaction do
      if linked_flows.present?
        linked_flows.each do |flow_node_id|
          flow_node_id = FlowNode.origin_node(flow_node_id)&.id
          input = {
            left_origin_node_id: self.origin_node_id,
            right_origin_node_id: flow_node_id,
            link_type: FlowLinkType.types[:linked_flow]
          }
          FlowLink.build_from_input(api_user_info, input)
        end
      end

      if retriever_data_sets.present?
        data_sets = DataSet.accessible_by_user(api_user_info.user, api_user_info.org, { access_role: :all, selected_ids: retriever_data_sets })
        raise Api::V1::ApiError.new(:not_found, "Data retriever not found or inaccessible") if data_sets.empty?

        data_sets.each do |data_set|
          next if data_set.origin_node_id.nil?
          next unless data_set.api_compatible?
          input = {
            left_origin_node_id: self.origin_node_id,
            right_origin_node_id: data_set.origin_node_id,
            link_type: FlowLinkType.types[:linked_flow],
            retriever_data_set_id: data_set.id
          }
          FlowLink.build_from_input(api_user_info, input)
        end
      end
    end
  end

  def remove_linked_flows (api_user_info, linked_flows, retriever_data_sets = nil, all = false)
    linked_flows = self.linked_flow_ids if all
    linked_flows&.each do |flow_node_id|
      l_id = self.origin_node_id
      r_id = FlowNode.origin_node(flow_node_id)&.id

      fl = FlowLink.where(left_origin_node_id: [l_id, r_id],
        right_origin_node_id: [l_id, r_id], link_type: FlowLinkType.types[:linked_flow])

      next if fl.empty?

      if fl.any? { |l| !l.has_admin_access?(api_user_info.user) }
        raise Api::V1::ApiError.new(:forbidden, "Invalid access to flow link")
      end

      fl.destroy_all
    end

    if retriever_data_sets.present?
      retriever_data_sets.each do |data_retriever_id|
        data_set = DataSet.accessible_by_user(api_user_info.user, api_user_info.org, { access_role: :all, selected_ids: [data_retriever_id] }).first
        next if data_set.blank?

        l_id = self.origin_node_id
        r_id = data_set.origin_node_id

        fl = FlowLink.where(left_origin_node_id: [l_id, r_id], right_origin_node_id: [l_id, r_id],
          retriever_data_set_id: data_set.id, link_type: FlowLinkType.types[:linked_flow])

        next if fl.empty?

        if fl.any? { |l| !l.has_admin_access?(api_user_info.user) }
          raise Api::V1::ApiError.new(:forbidden, "Invalid access to flow link")
        end

        fl.destroy_all
      end
    end
  end

  def data_retriever_ids
    origin_node_id = self.origin_node_id

    FlowLink.where(left_origin_node_id: origin_node_id)
      .or(FlowLink.where(right_origin_node_id: origin_node_id))
      .where(link_type: FlowLinkType.types[:linked_flow])
      .where.not(retriever_data_set_id: nil)
      .pluck(:retriever_data_set_id)
      .uniq
  end
end
