class AiWebServiceCacheWorker
  include Sidekiq::Worker
  sidekiq_options retry: false

  def perform(flow_node_id)
    flow_node = FlowNode.find(flow_node_id)
    AiWebService.new.flush_cache(flow_node.org, flow_node.origin_node_id)
  end
end
