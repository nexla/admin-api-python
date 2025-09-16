class FlowNodesDocContainer < ActiveRecord::Base
  self.primary_key = :id

  belongs_to :flow_node
  belongs_to :doc_container

  def self.transfer_from_data_flows (res_key, res)
    if defined?(DataFlowsDocContainer)
      if (!res.respond_to?(:flow_node_id) || res.flow_node.nil? || res.origin_node.nil?)
        raise Api::V1::ApiError.new(:internal_server_error,
          "Invalid resource for flow doc container. Missing flow node: #{res.class.name}, #{res.id}")
      end

      DataFlowsDocContainer.where(res_key => res.id).each do |dfdc|
        FlowNodesDocContainer.where({
          flow_node_id: res.flow_node_id,
          doc_container_id: dfdc.doc_container_id
        }).first_or_create
      end
    end
  end
end