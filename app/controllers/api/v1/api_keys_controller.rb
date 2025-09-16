module Api::V1  
  class ApiKeysController < Api::V1::ApiController      
    MODELS = [DataSetsApiKey, DataSinksApiKey, DataSourcesApiKey, UsersApiKey].freeze

    def index
      @api_keys = Hash.new
      MODELS.each do |model|
        type_key = model.table_name.gsub("_api_keys", "").to_sym
        @api_keys[type_key] = model.where(:owner => current_user, :org => current_org)
      end

      paginate(@api_keys)
    end

    def show
      MODELS.each do |model|
        @api_key = model.find_by_api_key(params[:id]) || model.find_by_id(params[:id])
        if @api_key.present?
          @resource_attribute = model.table_name.gsub("s_api_keys", "_id")
          break
        end
      end

      head :not_found and return if @api_key.nil?

      @show_dataplane_details = false
      if !current_user.super_user?
        head :forbidden and return if (@api_key.org_id != current_org.id)
        if !current_org.has_admin_access?(current_user)
          head :forbidden and return if (@api_key.owner_id != current_user.id)
        end
      else
        @show_dataplane_details = true
      end
    end

    def search
      @api_keys = Hash.new
      MODELS.each do |model|
        scope = model.where(:owner => current_user, :org => current_org)
        type_key = model.table_name.gsub("_api_keys", "").to_sym
        result = Common::Search::BasicSearchExecutor.new(current_user, current_org, model, params[:filters], scope).call
        @api_keys[type_key] = result
      end

      paginate(@api_keys)
      render 'index'
    end

    private
    def paginate(api_keys_hash)
      api_keys_hash.each{|k,v| api_keys_hash[k] = v.paginate(page: @page, per_page: @per_page) }
      longest_collection = api_keys_hash.values.max_by(&:total_entries)
      set_link_header(longest_collection)
    end
  end
end
