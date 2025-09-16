module Api::V1  
  class UsersController < Api::V1::ApiController      
    include PaperTrailControllerInfo
    include ApiKeyConcern

    skip_before_action :authenticate, only: [:show_sso_options, :password_entropy]
    before_action only: [:audit_log, :audit_history] do
      initialize_date_params(:current_and_previous_q)
    end
    
    def index
      if (params[:email].present? && params[:email].match(URI::MailTo::EMAIL_REGEXP))
        head :bad_request and return if !current_user.super_user?
        @user = User.find_by_email(params[:email])
        head :not_found and return if @user.nil?
        render "show" and return
      end

      resource_select = current_user.users(request_access_role, current_org)
      users = add_request_filters(resource_select, User).page(@page).per_page(@per_page)
      if (request_access_role == :all && current_user.infrastructure_or_super_user?)
        # Cache org memberships, access roles, and Nexla admin ids for 
        # quicker rendering, and disallow &expand=1 for this type of request.
        @org_member_roles = Org.all_member_roles(current_org)
        @expand = false
      end

      # If displaying the User record that matches @api_user,
      # use that instance so that we display the correct additional
      # information (e.g. impersonated?, impersonator, etc.)
      # See NEX-11840.
      @users = users.map { |u| (u.id == current_user.id) ? current_user : u }
      set_link_header(@users)
    end
    
    def show
      return if render_schema User
      # If displaying the User record that matches @api_user,
      # use that instance so that we display the correct additional
      # information (e.g. impersonated?, impersonator, etc.)
      # See NEX-11840.
      @user = (params[:id].to_i == current_user.id) ? current_user : User.find(params[:id])
      authorize! :read, @user
    end

    def show_sso_options
      # Note, @resource must respond to :sso_options
      @resource = nil
      if (params.key?(:email))
        email = params[:email]
        head :bad_request and return if (!email.is_a?(String))
        head :bad_request and return if email.match(URI::MailTo::EMAIL_REGEXP).nil?
        @resource = User.find_by_email(email)
      elsif (params.key?(:client_identifier))
        @resource = Org.find_by_client_identifier(params[:client_identifier])
      end
      raise Api::V1::ApiError.new(:not_found) if @resource.nil?
      render "show_sso_options"
    end

    def create
      validate_create
      input = (validate_body_json User).symbolize_keys
      @user = User.build_from_input(input, current_user, current_org)
      render "show"
    end

    def login_history
      @user = User.find(params[:user_id])
      authorize! :read, @user
      if (params[:event_type].blank? || params[:event_type] == "login_history")
        @login_history = @user.login_audits.page(@page).per_page(@per_page)
      end
      if (params[:event_type].blank? || params[:event_type] == "logout_history")
        @logout_history = @user.logout_audits.page(@page).per_page(@per_page)
      end
    end

    def api_key_events
      @user = User.find(params[:user_id])
      authorize! :read, @user
      @api_key_events = @user.api_key_events
      @api_key_events = @api_key_events.where(api_key_type: params[:resource_type]) if params[:resource_type].present?
      @api_key_events = @api_key_events.page(@page).per_page(@per_page)
    end

    def update
      if !(validate_update)
        head :method_not_allowed and return
      end
      input = (validate_body_json User).symbolize_keys
      @user = User.find(params[:id])
      authorize! :manage, @user
      if input.key?(:action)
        if input[:action].to_s.downcase == "source_activate"
          Org.activate_rate_limited_sources!(@user, @user.user_tier, true)
        else
          Org.pause_rate_limited_sources!(@user, @user.user_tier, input[:status],  0, true)
        end
      else
        @user.update_mutable!(request, current_user, current_org, input)
      end
      render "show"
    end

    def change_password
      @user = User.find(params[:user_id])
      authorize! :manage, @user
      @api_org = (@user.default_org || OrgMembership.where(user_id: @user.id, status: OrgMembership::Statuses[:active]).first)

      # Set the org context for the user, as we are not going through the
      # normal authentication path here. Also, render expects @api_user to 
      # be present.
      @user.org = current_org
      Current.set_user(@user)

      input = MultiJson.load(request.raw_post)
      input.symbolize_keys!
      if (input[:password].blank? || input[:password_confirmation].blank?)
        head :bad_request and return
      end
      validate_nexla_password_login(@user.default_org, @user)
      @user.change_password(input[:password], input[:password_confirmation])
      render "show"
    end

    def reset_password
      input = MultiJson.load(request.raw_post)
      input.symbolize_keys!

      head :bad_request and return if input[:g_captcha_response].blank? || input[:email].blank?

      input[:ip] = request.ip
      input[:remote_ip] = request.remote_ip
      input[:origin] = request.headers['origin']

      1.times do
        break if input[:email].blank?
        user = User.find_by_email(input[:email])
        break if user.nil?

        org = Org.find_by_id(input[:org_id] || user.default_org_id)
        break if org.nil?

        validate_nexla_password_login(org, user)
        result = user.create_password_reset_token(org, input[:origin])
      end
      head :ok
    end

    def audit_history
      @user = User.find(params[:user_id])
      authorize! :read, @user
      @date_interval = DateInterval.new("last_day") if @date_interval.is_blank?
      @audit_entries = AuditEntry.new(current_org).all(@date_interval, { owner_id: @user.id }).paginate(:page => @page, :per_page => @per_page)
      set_link_header(@audit_entries)
      render "api/v1/audit_entries/audit_log"
    end

    def audit_log
      @user = User.find(params[:user_id])
      authorize! :read, @user
      raise Api::V1::ApiError.new(:bad_request) unless ConstantResolver.instance.versioned_models.include?(params[:resource_type].to_sym)
      @audit_entries = AuditEntry.new(current_org).log_for_resource(params[:resource_type], @date_interval, { owner_id: @user.id })
      event_filter, negate = AuditEntryFilterValidator.validate_event_filter(params)
      if event_filter.present?
        @audit_entries = @audit_entries.select { |v|
          has = (v.event == event_filter)
          (negate ? !has : has)
        }
      end
      @audit_entries = @audit_entries.paginate(:page => @page, :per_page => @per_page)
      set_link_header(@audit_entries)
      render "api/v1/audit_entries/audit_log"
    end

    def set_password
      input = MultiJson.load(request.raw_post)
      input.symbolize_keys!

      head :bad_request and return if input[:reset_token].blank? ||
        input[:password].blank? || input[:password_confirmation].blank?

      @user = User.verify_password_reset_token(input[:reset_token])
      raise Api::V1::ApiError.new(:bad_request, "Invalid reset token") if @user.nil?
      validate_nexla_password_login(@user.default_org, @user)

      @user.change_password(input[:password], input[:password_confirmation])
      Current.set_user(@user)
      current_user.org = @api_org = @user.default_org

      render "show"
    end

    def lock_account
      head :forbidden and return if !current_user.super_user?
      @user = User.find(params[:user_id])
      @user.lock_account
      render "show"
    end

    def unlock_account
      head :forbidden and return if !current_user.super_user?
      @user = User.find(params[:user_id])
      @user.unlock_account
      render "show"
    end

    def activate
      input = MultiJson.load(request.raw_post) if !request.raw_post.blank?
      input ||= {}
      input.symbolize_keys!

      # Only Nexla admins can deactivate user's account without an Org context
      head :forbidden and return if input[:org_id].blank? && !current_user.super_user?

      org = nil

      if (input[:org_id].blank?)
        head :forbidden and return if !current_user.super_user?
      else
        org = Org.find_by_id(input[:org_id])
        head :not_found and return if org.nil?
        head :forbidden and return if !org.has_admin_access?(current_user)
      end

      @user = User.find_by_id(params[:user_id])
      head :not_found and return if @user.nil?
      if (!org.nil?)
        om = OrgMembership.where(:user => @user, :org => org).first
        head :bad_request and return if om.nil?
      end

      if params[:activate]
        @user.activate!(org)
      else
        return if process_async_request("DeactivateUser", user_id: @user.id)

        @user.deactivate!(org, input[:pause_data_flows])
      end

      render "show"
    end

    def destroy
      head :method_not_allowed
    end

    def orgs
      @user = User.find(params[:user_id])
      authorize! :read, @user
      render "orgs"
    end

    def metrics
      user = User.find(params[:user_id])
      authorize! :read, user

      if (!params[:metrics_name].nil? && params[:metrics_name] != "lineage")
        org = Org.find(params[:org_id])
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to resource") if !user.org_member?(org)
      end

      params[:metrics_name] ||= "org_owner_aggregate"
      result = MetricsService.new.get_metric_data(user, params)
      if (!params[:metrics_name].nil? && params[:metrics_name] == "lineage")
        result = result.deep_symbolize_keys
        result[:output] = format_lineage_response(result[:metrics], current_user, current_org)
        result.delete(:metrics)
      end
      render :json => result, :status => result[:status]
    end
    
    def get_tags
      user = User.find(params[:user_id])
      authorize! :read, user
      render :json => user.owned_tags.map(&:name)
    end

    def activate_rate_limited_sources
      @user = User.find(params[:user_id])
      authorize! :manage, @user
      if params[:activate]
        Org.activate_rate_limited_sources!(@user, @user.user_tier, true)
      else
        Org.pause_rate_limited_sources!(@user, @user.user_tier, params[:status],  0, true)
      end
      render "show"
    end

    def account_rate_limited
      user = User.find(params[:user_id])
      authorize! :read, user
      limited = user.user_tier.present? ? (user.status == Org::Statuses[:source_data_capped]) : false
      render :json => { :account_rate_limited => limited }
    end

    def transferable
      user = User.find(params[:user_id])
      authorize! :read, user
      if params.key?(:org_id)
        org = params[:org_id].blank? ? nil : Org.find(params[:org_id])
      else
        org = user.default_org
      end
      if org.present? && !user.org_member?(org)
        head :forbidden
      else
        render :json => user.transferable(org)
      end
    end

    def transfer
      user = User.find(params[:user_id])
      authorize! :manage, user

      async_params = {
        from_user_id: user.id,
        to_user_id: params[:delegate_owner_id],
        from_org_id: params[:org_id],
        to_org_id: params[:delegate_org_id]
      }
      return if process_async_request("ChownUserResources", async_params)

      if !params[:delegate_owner_id].present?
        # Note, nil is not a valid value for this attribute
        raise Api::V1::ApiError.new(:bad_request, "Required delegate_owner_id missing from input")
      end
      delegate_owner = User.find(params[:delegate_owner_id])

      if !params.key?(:org_id)
        raise Api::V1::ApiError.new(:bad_request, "Required org_id missing from input")
      end
      org = params[:org_id].blank? ? nil : Org.find(params[:org_id])

      delegate_org = params[:delegate_org_id].blank? ? nil : Org.find(params[:delegate_org_id])
      if delegate_org.present?
        raise Api::V1::ApiError.new(:forbidden) if !current_user.super_user?
      end

      if org.present?
        if delegate_org.present?
          raise Api::V1::ApiError.new(:forbidden) if !delegate_owner.org_member?(delegate_org)
          summary = user.transfer(org, delegate_owner, delegate_org)
        else
          raise Api::V1::ApiError.new(:forbidden) if !delegate_owner.org_member?(org)
          summary = user.transfer(org, delegate_owner)
        end
      else
        if !current_user.super_user? && (current_user.id != user.id)
          raise Api::V1::ApiError.new(:forbidden)
        end
        summary = user.transfer(nil, delegate_owner)
      end
      render :json => summary
    end

    def account_summary
      user = User.find(params[:user_id])
      authorize! :read, user
      if params[:org_id].present?
        org = Org.find(params[:org_id])
        authorize! :read, org
      else
        org = user.default_org
      end

      unless user.org_member?(org)
        raise Api::V1::ApiError.new(:bad_request, "User is not a member of the specified org")
      end

      render json: AccountSummary::StatsService.new(org, user).call
    end

    def current_user_account_summary
      if params[:org_id].present?
        org = Org.find(params[:org_id])
        authorize! :read, org
      else
        org = current_org
      end

      unless current_user.org_member?(org)
        raise Api::V1::ApiError.new(:bad_request, "User is not a member of the specified org")
      end

      render json: AccountSummary::StatsService.new(org, current_user).call
    end

    def password_entropy
      render json: User.validate_password(params[:email] || current_user&.email,
                                          params[:full_name] || current_user&.full_name,
                                          params[:password])
    end

    def send_invite
      raw_body = request.raw_post.present? && JSON.parse(request.raw_post) rescue nil
      invitee_email = params[:invitee_email] || raw_body&.dig("invitee_email")
      unless invitee_email.present? && invitee_email.match(URI::MailTo::EMAIL_REGEXP)
        raise Api::V1::ApiError.new(:bad_request, "Invalid email address")
      end

      if current_org.self_signup? && current_org.members.count >= current_org.members_limit
        raise Api::V1::ApiError.new(:bad_request, "You can invite only #{current_org.members_limit - 1} user(s)")
      end

      invite = Invite.create!(invitee_email: invitee_email, org_id: current_org.id, created_by_user_id: current_user.id)
      NotificationService.new.publish_invite(current_user, invitee_email, request.origin, invite.uid)

      render json: { result: :ok }
    end

    def current
      @api_org_membership = OrgMembership.where(:org => current_org, :user => current_user)[0] if !current_org.nil?
      @admin_access = can?(:manage, Org)
    end

    protected

    def validate_create
      # Super users can create users in any context, non-super users
      # can do so only within an Org they administer
      return true if current_user.super_user?
      return true if !current_org.nil? && current_org.has_admin_access?(current_user)
      return false
    end

    def validate_update
      # Users can always update their own records.
      # To modify other users, creation rules apply.
      return true if (current_user.id.to_i == params[:id].to_i)
      validate_create
    end

    def format_lineage_response(lineage_data, user, org)
      return {} if lineage_data.blank?

      response = DataFlow.empty_flows
      response.delete(:flows)

      acl_lineage = lineage_data
      acl_lineage = acl_lineage.deep_symbolize_keys
      acl_lineage.delete(:resources)
      acl_lineage[:resources] = []
      lineage_data = lineage_data.deep_symbolize_keys
      if !lineage_data[:resources].blank?
        lineage_data[:resources].each do |resource|
          case resource[:resource_type]
            when "data_source"
              data_source = DataSource.find_by_id(resource[:id])
              if (can? :read, data_source)
                resource[:connection_type] = resource[:connection_type].to_s.downcase if !resource[:connection_type].blank?
                acl_lineage[:resources] << resource
                response[:data_sources] << data_source.flow_resource_data(response, user, org)
              end
            when "data_set"
              data_set = DataSet.find_by_id(resource[:id])
              if (can? :read, data_set)
                acl_lineage[:resources] << resource
                data_set.flow_node_data(response, user, org)
              end
            when "data_sink"
              data_sink = DataSink.find_by_id(resource[:id])
              if (can? :read, data_sink)
                resource[:connection_type] = resource[:connection_type].to_s.downcase if !resource[:connection_type].blank?
                acl_lineage[:resources] << resource
                response[:data_sinks] << data_sink.flow_resource_data(response, user, org)
              end
          end
        end
      end

      response[:data_sources].uniq!
      response[:data_sets].uniq!
      response[:data_sinks].uniq!
      response[:data_credentials].uniq!
      response[:orgs].uniq!
      response[:users].uniq!
      response[:lineage] = acl_lineage
      return response
    end

  end
end


