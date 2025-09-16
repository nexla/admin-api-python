module Api::V1  
  class DataMapsController < Api::V1::ApiController
    include PaperTrailControllerInfo
    include DocsConcern
    include AccessorsConcern

    using Refinements::HttpResponseString

    def index
      @validate = params[:validate].truthy?
      options = {
        access_role: request_access_role,
        access_roles: @access_roles
      }

      @data_maps = add_request_filters(
        current_user.data_maps(current_org, options), DataMap
      ).page(@page).per_page(@per_page)
      set_link_header(@data_maps)

      load_tags(@access_roles)
    end

    def public
      @validate = params[:validate].truthy?
      @data_maps = add_request_filters(DataMap.where(:public => true), DataMap).page(@page).per_page(@per_page)
      set_link_header(@data_maps)
      render "index"
    end
    
    def show
      @validate = params[:validate].truthy?
      return if render_schema DataMap
      @data_map = DataMap.find(params[:id])
      authorize! :read, @data_map
    end
    
    def create
      input = (validate_body_json DataMap).symbolize_keys

      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @data_map = DataMap.build_from_input(api_user_info, input)

      @data_map.update_mutable!(api_user_info, input)
      ResourceTagging.after_create_tagging(@data_map, input, current_user)
      render "show"
    end
    
    def update
      input = (validate_body_json DataMap).symbolize_keys
      @data_map = DataMap.find_by_id(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @data_map.nil?
      authorize! :manage, @data_map

      api_user_info = ApiUserInfo.new(current_user, current_org, input, @data_map)
      @data_map.update_mutable!(api_user_info, input)
      render "show"
    end
    
    def destroy
      head :method_not_allowed
    end

    def validate
      @data_map = DataMap.find(params[:data_map_id])
      authorize! :read, @data_map
      result = TransformService.new.validate_data_map(@data_map)
      render_external_result(result)
    end

    def get_entries
      data_map = DataMap.find(params[:data_map_id])
      authorize! :read, data_map
      if request.post?
        input = JSON.parse(request.raw_post)
        result = TransformService.new.get_map_entries_by_post(data_map, input)
      else
        result = TransformService.new.get_map_entries(data_map, params[:keys])
      end
      render_external_result(result)
    end

    def set_entries
      input = (validate_body_json DataMap).symbolize_keys
      data_map = DataMap.find(params[:data_map_id])
      authorize! :manage, data_map
      result = data_map.set_map_entries(input)

      if result[:status].to_s.success_code?
         entries = input[:entries] || input[:data_map]
         render json: { message: "Entries updated successfully" }
       else
        render_external_result(result)
      end
    end

    def delete_entries
      input = request.raw_post.present? ? MultiJson.load(request.raw_post) : nil
      if input.present? && !(input.is_a?(Array) || input.all?{|k| k.is_a?(String) })
        raise Api::V1::ApiError.new(:bad_request, "Input must be an array of strings")
      end

      data_map = DataMap.find(params[:data_map_id])
      authorize! :manage, data_map

      result = data_map.delete_map_entries(input || params[:keys], use_post: request.raw_post.present?)
      head result[:status]
    end

    def download_map
      @data_map = DataMap.find(params[:data_map_id])
      authorize! :read, @data_map
      result = TransformService.new.download_map(@data_map)
      render plain: result[:output], status: result[:status]
    end

    def search_tags
      input = MultiJson.load(request.raw_post)
      @data_maps = ResourceTagging.search_by_tags(DataMap, input, current_user, request_access_role, current_org)
      set_link_header(@data_maps)
      render "index"
    end

    def search
      sort_opts = params.slice(:sort_by, :sort_order)
      scope = current_user.data_maps(current_org, { access_role: request_access_role, access_roles: @access_roles })
      scope = Common::Search::BasicSearchExecutor.new(current_user, current_org, DataMap, params[:filters], scope, sort_opts: sort_opts).call
      @data_maps = scope.page(@page).per_page(@per_page)
      set_link_header(@data_maps)
      render :index
    end

    private
    def render_external_result(result)
      if result[:status].to_s.success_code?
        render json: result[:output], status: result[:status]
      else
        render json: { message: result[:message] || "Transform Service error" }, status: result[:status] || 500
      end
    end
  end
end


