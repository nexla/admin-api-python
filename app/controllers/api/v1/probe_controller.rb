module Api::V1
  class ProbeController < Api::V1::ApiController
    before_action -> { authorize! :read, resource }

    def test_authenticate
      async_params = {
        resource_type: self.class.model_class.name,
        resource_id: resource.id,
        action: "authenticate"
      }
      return if process_async_request("CallProbe", async_params)

      # Note, not using the name "authenticate" here, for
      # clarity and to avoid overriding ApiController::authenticate
      result = probe_service.authenticate
      render :json => result, :status => result[:status]
    end

    def summary
      result = probe_service.authenticate
      render :json => result, :status => result[:status]
    end

    def list_tree
      async_params = {
        resource_type: self.class.model_class.name,
        resource_id: resource.id,
        action: "list_tree",
        input: input
      }
      return if process_async_request("CallProbe", async_params)

      result = probe_service.list_tree(map_paging_params(input))
      render :json => result, :status => result[:status]
    end

    def list_buckets
      async_params = {
        resource_type: self.class.model_class.name,
        resource_id: resource.id,
        action: "list_buckets"
      }
      return if process_async_request("CallProbe", async_params)

      result = probe_service.list_buckets
      result[:buckets] = result.delete(:output)
      render :json => result, :status => result[:status]
    end

    def list_files
      async_params = {
        type: self.class.model_class.name,
        id: resource.id,
        action: "list_files",
        input: input
      }
      return if process_async_request("CallProbe", async_params)

      result = probe_service.list_files(input)
      render :json => result, :status => result[:status]
    end

    def read_file
      result = probe_service.read_file(input["path"], input["file"])

      temporary_preocess_of_file_result(result)

      render :json => result, :status => result[:status]
    end

    def read_sample
      async_params = {
        type: self.class.model_class.name,
        id: resource.id,
        action: "read_sample",
        input: input
      }
      return if process_async_request("CallProbe", async_params)

      result = probe_service.read_sample(input, @page, @per_page, request, current_user)
      render :json => result, :status => result[:status]
    end

    def read_quarantine
      async_params = {
        type: self.class.model_class.name,
        id: resource.id,
        action: "read_quarantine_sample",
        input: input
      }
      return if process_async_request("CallProbe", async_params)

      result = probe_service.read_quarantine_sample(input)
      render :json => result, :status => result[:status]
    end

    def detect_schemas
      async_params = {
        type: self.class.model_class.name,
        id: resource.id,
        action: "detect_schemas",
        input: input
      }
      return if process_async_request("CallProbe", async_params)

      result = probe_service.detect_schemas(input)
      output = result[:output]
      output = output[:output] if output.is_a?(Hash)
      render :json => output, :status => result[:status]
    end

    def search_path
      keep_path = true
      if input.key?(:keep_path)
        keep_path = input[:keep_path]
      elsif params.key?(:keep_path)
        keep_path = params[:keep_path].truthy?
      end
      query = input[:query] || params[:query]

      result = probe_service.search_path(input, query, keep_path)

      render :json => result, :status => result[:status]
    end

    class << self
      def setup_probe_service_for(model_class)
        @model_class = model_class
      end

      def model_class
        raise NotImplementedException unless @model_class

        @model_class
      end
    end

    memoize def resource
      id_param = (self.class.model_class.name.singularize.underscore + "_id").to_sym
      self.class.model_class.find(params[id_param])
    end

    memoize def probe_service
      ProbeService.new(resource)
    end

    def map_paging_params (input)
      {
        offset: input.delete('page'),
        pageSize: input.delete('per_page')
      }.compact.merge(input)
    end

    def input
      return {} if request.raw_post.empty?

      MultiJson.load(request.raw_post).with_indifferent_access
    end

    def temporary_preocess_of_file_result(result)
      # TODO: Check if this temporary solution should be resolved somehow
      ## TEMPORARY (10/2016) massage the output of the backend Probe Service a bit...
      return unless (result[:status] == :ok && !result[:output].nil?)

      result[:output].symbolize_keys!.tap do |output|
        break unless output[:messages].is_an?(Array)

        output[:records] = output.delete(:messages).pluck('rawMessage')
      end
    end
  end
end
