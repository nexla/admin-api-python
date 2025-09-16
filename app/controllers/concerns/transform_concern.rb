module TransformConcern
  extend ActiveSupport::Concern

  using Refinements::SchemaProperty

  included do
    helper_method :transform
    helper_method :transform_features
  end

  def transform_features
    result = TransformService.new.features
    render :json => result, :status => result[:status]
  end

  def transform
    input = request.raw_post.empty? ? {} : MultiJson.load(request.raw_post, mode: :compat)

    if input["spec"].present?
      result = TransformService.new.transform_with_spec(input, current_org)
      render status: result[:status], json: result
      return
    end

    transform = nil
    annotations = nil
    output_validation_schema = nil

    case
    when params.key?(:transform_id)
      tx = Transform.find(params[:transform_id])
      authorize! :read, tx
      transform = tx.get_jolt_spec
    when params.key?(:data_set_id)
      ds = DataSet.find(params[:data_set_id])
      authorize! :transform, ds
      transform = ds.transform
      annotations = ds.output_schema_annotations
      output_validation_schema = ds.output_validation_schema
    else
      transform = input["transform"]
    end

    raise Api::V1::ApiError.new(:bad_request, "Invalid transform code!") if transform.nil?
    annotations ||= input["output_schema_annotations"]
    output_validation_schema ||= input["output_validation_schema"]
    input = input["input"]

    render :json => { :status => :bad_request } and return if transform.nil? || input.nil?

    result = {
      :status => :ok,
      :transform => transform,
      :input => input
    }

    options = {
      extract_schemas: params[:extract_schemas].truthy?,
      accumulate_schema: params[:accumulate_schema].truthy?,
      suppress_errors: params[:suppress_errors],
      org: current_org
    }

    options[:schema] = DataSet.add_validation_schema_info(output_validation_schema) if !output_validation_schema.blank?

    tx = TransformService.new.transform(transform, input, options)

    # TODO Notice: networking/connection errors are not processed
    result.merge!(tx.slice(:status, :output, :schemas, :error))

    if (!result[:schemas].blank? && !annotations.blank?)
      merged_schemas = []
      result[:schemas].each do |schema|
        merged_schemas << schema.schema_merge_annotations(annotations)
      end
      result[:schemas] = merged_schemas
    end

    render :json => result.compact, :status => result[:status]
  end

end
