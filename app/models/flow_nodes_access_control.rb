class FlowNodesAccessControl < ActiveRecord::Base
  self.primary_key = :id

  include AuditLog
  include AccessControlUtils

  belongs_to :flow_node, required: true

  def self.transfer_from_data_flows (res_key, res)
    if (!res.respond_to?(:flow_node_id) || res.flow_node.nil? || res.origin_node.nil?)
      raise Api::V1::ApiError.new(:internal_server_error,
        "Invalid resource for flow access control entry. Missing flow node: #{res.class.name}, #{res.id}")
    end
    
    DataFlowsAccessControl.where(res_key => res.id).each do |dfac|
      # flow_nodes_access_control entries always refer
      # to the origin node of the flow, regardless of the
      # specific node used when adding the access rule.
      FlowNodesAccessControl.where({
        flow_node_id: res.origin_node_id,
        accessor_id: dfac.accessor_id,
        accessor_type: dfac.accessor_type,
        accessor_org_id: dfac.accessor_org_id,
        role_index: dfac.role_index
      }).first_or_create
    end
  end
end
