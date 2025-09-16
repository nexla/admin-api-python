class FlowDeleteWorker < BaseWorker
  sidekiq_options queue: 'flows', retry: 3

  def perform (flow_node_id)
    # Note, not requiring an origin node id here as
    # this worker will primarily handle deleting flows
    # starting from auto-generated data sources, which
    # have the upstream origin_node_id.
    fn = FlowNode.find_by_id(flow_node_id)
    if fn.nil?
      # Log this? We don't want to retry, there's no
      # point. But we might want to see it in a log.
      ActiveRecord::Base.logger.info("WARNING: FlowDeleteWorker did not find flow_node_id: #{flow_node_id}")
    else
      # Note, downstream only
      fn.flow_pause!(all: false)
      fn.flow_destroy(all: false)
    end
  end
end
      
