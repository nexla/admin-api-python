module Api::V1
  class ApiController < ApplicationController
    NOT_LOGGED_ERROR_STATUSES = [:unauthorized, :not_found, :locked, :bad_request, :forbidden, :method_not_allowed].freeze
    NOT_LOGGED_EXCEPTIONS = [ActiveRecord::RecordNotFound ].freeze

    include Api::V1::Auth
    include RequestFilters

    extend Memoist

    rescue_from StandardError,   with: :handle_exceptions
    rescue_from Exception,       with: :handle_exceptions

    rescue_from CanCan::AccessDenied do |e|
      handle_exceptions(Api::V1::ApiError.new(:forbidden))
    end

    rescue_from MultiJson::LoadError do |e|
      logger.info("API: rescue_from LoadError: #{e.message}, #{e.backtrace}")
      handle_exceptions(Api::V1::ApiError.new(:bad_request, "Cannot parse JSON request body"))
    end

    rescue_from ActiveRecord::StatementTimeout do |e|
      handle_exceptions(Api::V1::ApiError.new(:internal_server_error,
        "Request did not complete due to a service timeout"))
    end

    skip_before_action  :verify_authenticity_token
    before_action do |controller|
      request.headers['req_time'] = Time.now.utc
      response.headers["Pragma"] = "no-cache"
      response.headers["Cache-Control"] = "no-store"
      validate_query_parameters
    end

    before_action do
      # This action must come before :authenticate. It CANNOT be
      # inside :authenticate, because not all endpoints use :authenticate
      # out-of-the-box. Some endpoints have custom authentication code
      # for handling non-standard requests (see /resource_authorize, for example).
      # But ALL request handling should clear previous Current context.
      # See NEX-17969 for why we don't do this after 
      # the "yield" in set_current_values().
      Current.clear!
    end

    before_action   :authenticate, :except => [:status, :raise_no_route!, :reset_password, :set_password]
    around_action   :set_current_values, :except => [:status, :raise_no_route!]
    before_action   :reset_notifications_lock

    # After preflight callback is defined

    include PaginateConcern

    def set_current_values
      preflight

      Current.set_request(request)

      yield
    end

    after_action :request_logging

    # Hack Alert - to avoid duplicating ActiveRecord calls
    # with and without paginators, we *always* use the paginator,
    # and, if the caller didn't ask for pagination, we use this
    # rather large number for the per_page value (which translates
    # into the second argument in the SQL LIMIT clause).
    PAGINATE_ALL_COUNT = 2**32

    def authenticate(optional = false)
      1.times do
        break if !valid_request_origin?

        auth = request.headers['Authorization']
        break if auth.blank?

        auth_parts = auth.split
        raise Api::V1::ApiError.new(:bad_request,
          "Invalid header format") if (auth_parts.length < 2)

        return if verify_token(*auth_parts, request, logger)
      end

      unauthorized! unless optional
    end

    def authenticate_optional
      authenticate(true)
    end

    def valid_request_origin?
      return true if !Rails.env.production?
      load_whitelist_config
      req_host = request.protocol + request.host_with_port
      return (Rails.env.production? && (request.ssl? || (@@whitelist_config[:hosts]).include?(req_host)))
    end

    def load_whitelist_config
      @@whitelist_config ||= nil
      return if !@@whitelist_config.nil?
      @@whitelist_config = YAML.load_file(Rails.root.join('config/whitelist_host.yml'))[Rails.env]
      @@whitelist_config.symbolize_keys!
    end

    def set_cors_headers
      @@allowed_origins ||= nil
      @@allow_all_origins ||= nil

      if (@@allowed_origins.nil?)
        tmp = (ENV['ALLOWED_ORIGINS'] || "").split(",")
        @@allow_all_origins = tmp.include?("*")
        @@allowed_origins = tmp.select { |o| !o.include?("*") }
      end

      origin = request.headers['Origin']
      return if (!@@allow_all_origins && (origin.blank? || !@@allowed_origins.include?(origin)))
        
      response.headers['Access-Control-Allow-Origin'] = (@@allow_all_origins ? "*" : origin)
      response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PATCH, PUT, DELETE, OPTIONS, HEAD'
      response.headers['Access-Control-Allow-Headers'] = 
        '*, x-requested-with,Content-Type,Authorization,If-Modified-Since,If-None-Match'
      response.headers['Access-Control-Allow-Credentials'] = 'true'
      response.headers['Access-Control-Expose-Headers'] = 
        'Link, X-Total-Count, X-Current-Page, X-Page-Count, X-Total-Page-Count'
    end

    memoize def request_dataplane
      dataplane = nil
      dataplane_uid = request.headers[API_SECRETS[:dataplane_header]]&.downcase
      if dataplane_uid.present?
        dataplane = Cluster.find_by_uid(dataplane_uid)
        raise Api::V1::ApiError.new(:bad_request, "Dataplane not present: #{dataplane_uid}") if dataplane.nil?
      end
      dataplane
    end

    def preflight
      @api_root = "api/v1/"
      @access_roles = Hash.new
      @tags = Hash.new
      RequestStore.store[:flows] = DataFlow.empty_cache
      @expand = params[:expand].truthy?
      @brief = params[:brief].truthy?
      @include_summary = params[:include_summary].truthy?

      @page = (params[:page].is_a?(Integer) || params[:page].is_a?(String)) ? params[:page].to_i : 0
      @per_page = (params[:per_page].is_a?(Integer) || params[:per_page].is_a?(String)) ? params[:per_page].to_i : 0
      @paginate = (@page > 0) || (@per_page > 0)
      @page = 1 if @page <= 0
      if @paginate
        @per_page = 10 if @per_page <= 0
      else
        @per_page = PAGINATE_ALL_COUNT
      end

      # Due to how callbacks works, we need to trigger it from this method.
      # As it needs to be called after pagination is done
      enforce_pagination_for_endpoint

      @query_results = nil
      set_cors_headers
      request.format = 'json'

      RequestStore.store[:controller_request_info] = {
        request_ip: request.remote_ip,
        request_url: request.url,
        request_method: request.method,
        request_referer: request.referer,
        request_user_agent: request.user_agent.to_s.downcase
      }
    end

    def reset_notifications_lock
      Notifications::ResourceNotifier.reset_exclusive_resource
    end

    def load_tags (access_roles)
      access_roles.keys.each do |res_type|
        @tags[res_type] = Hash.new
        access_roles[res_type].keys.each { |i| @tags[res_type][i] = Array.new }

        taggable_type = res_type.to_s.camelcase
        taggable_type = taggable_type.singularize if (res_type != :data_credentials)

        taggings = Tagging.where(taggable_type: taggable_type, taggable_id: access_roles[res_type].keys)
          .pluck(:taggable_id, :tag_id)

        tags = Tag.where(id: taggings.map(&:second)).pluck(:id, :name).to_h
        taggings.each do |e|
          @tags[res_type][e.first] << tags[e.second]
        end
      end
      @tags
    end

    def request_access_role (default = nil)
      access_role = params[:access_role]
      access_role ||= (default.nil? ? :owner_only : default)
      return AccessControls::validate_access_role(access_role.to_sym)
    end

    def set_link_header (paged)
      if (paged.respond_to?("total_entries"))
        response.headers['X-Total-Count'] = paged.total_entries.to_s
      end

      return if !@paginate || paged.nil? || paged.try(:current_page).nil?

      response.headers['X-Current-Page'] = paged.try(:current_page).to_s
      response.headers['X-Page-Count'] = paged.length.to_s
      response.headers['X-Total-Page-Count'] = ((paged.total_entries.to_f/@per_page).ceil).to_s

      @@app_id_str ||= nil
      if (@@app_id_str.nil?)
        @@app_id_str = Rails.configuration.x.api['app_id']
        @@app_id_str = @@app_id_str.blank? ? "" : "/#{@@app_id_str}"
      end

      url = request.original_url.
        gsub(request.original_fullpath, "#{@@app_id_str}#{request.original_fullpath}").
        gsub(/&?per_page=\d*/, "").
        gsub(/&?page=\d*/, "")

      url = url + "&" if url.last != "?"
      prv = paged.try(:previous_page)
      nxt = paged.try(:next_page)

      return if prv.nil? && nxt.nil?
      
      prv_link = url + "page=#{prv}&per_page=#{@per_page}" if prv
      nxt_link = url + "page=#{nxt}&per_page=#{@per_page}" if nxt

      hdr = ""
      hdr += "<" + prv_link + ">" if prv
      hdr += "; rel=\"Previous\"" if !hdr.empty?
      hdr += ", " if (!hdr.empty? && nxt)
      hdr += "<" + nxt_link + ">; rel=\"Next\"" if nxt
      response.headers['Link'] = hdr
    end

    def status
      status = { :status => "up" }
      begin
        Org.connection.execute('SELECT 1').to_a
      rescue Exception => e
        raise Api::V1::ApiError.new(:internal_server_error, "service connection failed: #{e.message}")
      end
      render :json => { status: "up" }, :status => :ok
    end

    def status_with_authentication
      status = { :status => "up" }
      begin
        status[:version] = ENV.fetch("API_VERSION", "unknown")
        status[:schema] = SchemaMigration.order(:version).last&.version

        if current_user.infrastructure_or_super_user?
          o = Org.get_nexla_admin_org
          status[:nexla_admin_org] = [o.id, o.name, o.cluster_id]
          c_attr = [:id, :name]
          c_attr << :uid if Cluster.column_names.include?("uid")
          status[:dataplanes] = Cluster.all.pluck(*c_attr)
        end
      rescue Exception => e
        raise Api::V1::ApiError.new(:internal_server_error, "service connection failed: #{e.message}")
      end
      render :json => status, :status => :ok
    end

    def handle_exceptions(e)
      if ENV['DEBUG_OUTPUT'].truthy?
        puts "Exception: #{e.message}"
        puts e.backtrace
      end

      if NOT_LOGGED_EXCEPTIONS.any?{|t| e.is_a?(t) } ||
        (e.is_a?(Api::V1::ApiError) && NOT_LOGGED_ERROR_STATUSES.include?(e.status))
        
        if e.respond_to?(:response) && e.response
          render :json => e.response, :status => e.status
          return
        elsif e.respond_to?(:status)
          head e.status
          return
        elsif e.is_a?(ActiveRecord::RecordNotFound)
          head :not_found
          return
        end
      end

      info = {
        user_id: current_user&.id,
        org_id: current_org&.id,
        message: e.message
      }

      if request.present?
        info[:host] = request.headers['origin'].nil? ? request.host : request.headers['origin']
        info[:user_agent] = request.user_agent.to_s.downcase
        info[:method] = request.method
        info[:path] = request.fullpath
      end

      info[:trace] = e.backtrace.blank? ? [] :
        e.backtrace.select { |l| l.include?(Rails.root.to_s) }.map {|l| l.gsub(Rails.root.to_s, "")}

      if (!e.respond_to?('response') || !e.respond_to?('status')) 
        case e.class.name
        when "ActiveRecord::RecordNotFound"
          e = Api::V1::ApiError.new(:not_found)
        when "ActiveRecord::NotNullViolation"
          msg = "Invalid resource. Are you missing a required field?"
          e = Api::V1::ApiError.new(:bad_request, msg)
        when "ActiveRecord::RecordInvalid"
          msg = e.message.blank? ? "Invalid resource. Are you missing a required field?" : e.message
          e = Api::V1::ApiError.new(:bad_request, msg)
        when "Mysql2::Error::ConnectionError"
          msg = "Database Connection Error."
          e = Api::V1::ApiError.new(:internal_server_error, msg)
        when "ActiveRecord::StatementInvalid"
          msg = "Oops, something went wrong. We'll check it out right away."
          e = Api::V1::ApiError.new(:internal_server_error, msg)
        when "ArgumentError"
          e = Api::V1::ApiError.new(:bad_request, e.message)
        when 'ActiveModel::ValidationError'
          e = Api::V1::ApiError.new(:bad_request, e.message)
        when 'ActionDispatch::Http::Parameters::ParseError'
          e = Api::V1::ApiError.new(:bad_request, "Cannot parse JSON request body", nil, e.message)
        else
          message = format_exception_short(e)
          e = Api::V1::ApiError.new(:internal_server_error, message)
        end
      end

      info[:status] = e.status
      Rails.configuration.x.error_logger.error(info.to_json)
      request_logging

      if (e.response)
        render :json => e.response, :status => e.status
      else
        head e.status
      end
    end

    def raise_no_route!
      if (request.method == 'OPTIONS')
        set_cors_headers
        head :no_content
      else
        raise Api::V1::ApiError.new(:not_found)
      end
    end

    def validate_query_parameters
      begin
        errs = JSON::Validator.fully_validate(
          QueryParameter.schema(:get),
          request.query_parameters,
          :validate_schema => true
        )
      rescue => e
        raise Api::V1::ApiError.new(:bad_request, e.message)
      end
      raise Api::V1::ApiError.new(:bad_request, errs[0]) if (errs.length > 0)
    end

    def validate_query_id_list (value)
      id_list = value.split(",")
      begin
        errs = JSON::Validator.fully_validate(
          QueryParameter.schema(:get),
          { :id_list => id_list },
          :validate_schema => true
        )
      rescue => e
        raise Api::V1::ApiError.new(:bad_request, e.message)
      end
      raise Api::V1::ApiError.new(:bad_request, errs[0]) if (errs.length > 0)
      id_list
    end

    def validate_body_json (model, body_hash=nil)
      body_hash ||= MultiJson.load(request.raw_post, mode: :compat)
      errs = []

      if model.is_a?(Hash)
        sc = model
      else
        sc = (request.method == 'POST') ? model.schema(:post) : model.schema(:put)
      end

      begin
        errs = JSON::Validator.fully_validate(sc, body_hash, :validate_schema => true)
      rescue => e
        raise Api::V1::ApiError.new(:bad_request, e.message)
      end

      raise Api::V1::ApiError.new(:bad_request, errs[0]) if (errs.length > 0)
      return body_hash
    end

    def validate_input_hash (model, input_hash, req_method = nil)
      errs = []
      req_method = 'POST' if (req_method == :post)
      req_method ||= request.method

      begin
        errs = JSON::Validator.fully_validate(
          (req_method == 'POST') ? model.schema(:post) : model.schema(:put), 
          input_hash, :validate_schema => true
        )
      rescue => e
        raise Api::V1::ApiError.new(:bad_request, e.message)
      end

      raise Api::V1::ApiError.new(:bad_request, errs[0]) if (errs.length > 0)
      return input_hash
    end

    def render_schema (model)
      return false if (params['id'] != 'schema')

      if ["get", "post", "put"].include?(params['method'])
        schema = model.schema(params['method'].to_sym)
      else
        schema = {
          :get => model.schema(:get),
          :post => model.schema(:post),
          :put => model.schema(:put)
        }
      end

      render json: schema
      return true
    end

    def authenticate_with_api_key
      auth = request.headers['Authorization']
      if auth.blank?
        token = params[:api_key]
        auth_type = "Basic"
      else
        auth_type, token = auth.split
      end
      return if token.blank?
      return verify_basic_token auth_type, token
    end

    def request_logging
      RequestLogger.instance.log(current_user, current_org, request, response)
    rescue Exception => e
      logger.info "Error in request logging message: #{e.message}"
    end

    def format_exception_short(e)
      if Rails.env.development?
        path = e.backtrace[0]
        path.sub!(/.+admin-api/,'admin-api')
        "'#{e.message}' at #{path}"
      else
        e.message
      end
    end

    def unauthorized!
      raise Api::V1::ApiError.new(:unauthorized)
    end

    def per_page
      @per_page
    end

    def page
      @page
    end

    def collection
      @collection
    end

    def require_nexla_admin!
      unauthorized! unless current_user
      raise Api::V1::ApiError.new(:forbidden) unless current_user.super_user?
    end

    def process_async_request(task_type, input)
      if params[:async].truthy?
        task_instance = AsyncTasks::Manager.instantiate_shallow_task(task_type)
        if input.is_a?(ActionController::Parameters)
          input = input.to_unsafe_h
        end

        task_arguments = input.merge(request.query_parameters).symbolize_keys.slice(*task_instance.explain_arguments.keys)

        task_input = {
          task_type: task_type,
          arguments: task_arguments
        }

        @async_task = AsyncTask.build_from_input(current_user, current_org, task_input, request)

        AsyncTasks::Manager.start_job!(@async_task)

        render "api/v1/async_tasks/show_wrapped"
        return true
      end

      if params[:request_id].present?
        @async_task = AsyncTask.where(task_type: task_type).find(params[:request_id])
        authorize! :read, @async_task

        instance = AsyncTasks::Manager.instantiate_task(@async_task)
        if @async_task.completed? && instance.provides_download_link?
          url = @async_task.generate_presigned_url!
          redirect_to url
          return true
        end

        render "api/v1/async_tasks/show_wrapped"
        return true
      end

      false
    end

    memoize def paginated_collection
      collection.paginate(page: page, per_page: per_page)
    end

    helper_method :collection
    helper_method :paginated_collection
    helper_method :current_user
    helper_method :current_org

    def business_action(klass, **args)
      klass.new({performer: current_user, org: current_org}.merge(args))
    end

    def fail_with(type, message = nil)
      raise Api::V1::ApiError.new(type, message)
    end
  end
end
