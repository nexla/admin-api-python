class Api::V1::QuarantineAggregationController < Api::V1::ApiController
  using ::Refinements::HttpResponseString

  def trigger_aggregation
    if params[:run_id].blank?
      raise Api::V1::ApiError.new(:bad_request, "Run ID is required for triggering quarantine aggregation")
    end

    resource = find_resource(params[:resource_type], params[:resource_id])
    authorize! :read, resource

    result = QuarantineAggregationService.new.trigger_aggregation(resource, params[:run_id])
    if result[:status].to_s.success_code?
      head :ok
    else
      render json: result, status: result[:status]
    end
  end

  def get_aggregations
    resource = find_resource(params[:resource_type], params[:resource_id])
    authorize! :read, resource

    aggregations = QuarantineAggregationService.new.get_aggregations(resource)

    if aggregations[:status].to_s.success_code?
      render json: aggregations[:output]
    else
      render json: aggregations, status: aggregations[:status]
    end
  end

  private
  def find_resource(resource_type, resource_id)
    case resource_type.to_s
    when 'data_source'
      DataSource.find_by(id: resource_id)
    when 'data_set'
      DataSet.find_by(id: resource_id)
    when 'data_sink'
      DataSink.find_by(id: resource_id)
    else
      raise Api::V1::ApiError.new(:bad_request, "Unsupported resource type for quarantine aggregation: #{resource_type}")
    end
  end
end