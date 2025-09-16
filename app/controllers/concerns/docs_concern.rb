module DocsConcern
  extend ActiveSupport::Concern

  def docs
    model_name = params[:model].to_s
    mode = params[:mode].to_sym
    if (model_name == 'DataFlow')
      flow_params = { :user => current_user, :org => current_org }
      flow_params[:data_source_id] = params[:data_source_id]
      flow_params[:data_set_id] = params[:data_flow_id]
      flow_params[:data_sink_id] = params[:data_sink_id]
      resource = DataFlow.find(params)
    else
      model = model_name.constantize
      resource_id = (model_name.underscore.singularize + "_id").to_sym
      resource = model.find(params[resource_id])
    end

    if mode != :list
      input = (validate_body_json DocsList).symbolize_keys
    end

    if mode== :list
      authorize! :read, resource
    else
      authorize! :manage, resource
    end

    case mode
    when :reset
      resource.delete_docs(:all)
      add_docs(resource, input)
    when :add
      add_docs(resource, input)
    when :remove
      input[:docs].each do |doc|
        id = doc.is_a?(Integer) ? doc : (doc[:id] || doc["id"]).to_i
        dc = DocContainer.find_by_id(id)
        resource.delete_docs(dc) if !dc.nil?
      end
    end

    @doc_containers = resource.docs
    render "api/v1/doc_containers/index"
  end

  def add_docs (resource, input)
    return if !input.is_a?(Hash)
    api_user_info = nil

    ActiveRecord::Base.transaction do
      input[:docs].each do |doc|
        if (doc.is_a?(Integer))
          dc = DocContainer.find(doc)
          authorize! :read, dc
          resource.add_docs(dc)
        else
          DocContainer.validate_input_schema(doc, :post)
          api_user_info ||= ApiUserInfo.new(current_user, current_org, doc)
          dc = DocContainer.build_from_input(api_user_info, doc)
          resource.add_docs(dc) if dc.valid?
        end
      end
    end

  end

end