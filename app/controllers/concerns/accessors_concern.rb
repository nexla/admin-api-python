module AccessorsConcern
	extend ActiveSupport::Concern

  def accessors
    # Needed for tests
    params[:model] = (params[:model].constantize rescue nil) if params[:model].is_a?(String)
    params[:mode] = params[:mode].to_sym
    
    resource_id = (params[:model].name.underscore.singularize + "_id").to_sym
    @resource = params[:model].find(params[resource_id])

    if (params[:mode] == :list)
      authorize! :read, @resource
    else
      authorize! :manage, @resource
    end

    if (params[:mode] == :remove && request.raw_post.empty?)
      input = { :accessors => [] }
      params[:mode] = :reset
    elsif (params[:mode] != :list)
      input = (validate_body_json Accessors).symbolize_keys
    end

    case params[:mode].to_sym
    when :add
      @resource.add_accessors(input[:accessors])
    when :remove
      @resource.remove_accessors(input[:accessors])
    when :reset
      @resource.reset_accessors(input[:accessors])
    end

    render "api/v1/accessors/show"
  end
 
end