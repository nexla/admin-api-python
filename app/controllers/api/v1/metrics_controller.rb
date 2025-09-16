module Api::V1
  class MetricsController < Api::V1::ApiController
    def publish
      head :forbidden and return if !current_user.super_user?

      result = MetricsAggregationService.new.publish_raw(current_org, publish_parameters)
      render :json => result, :status => result[:status]
    end

    private

    def publish_parameters
      NumericParams.new(params.permit(:resourceId, :resourceType, :millis, :pipelineType, :tags, fields: {}).to_h.merge!({'orgId' => current_org.id, 'ownerId' => current_user.id})).to_h
    end
  end
end
