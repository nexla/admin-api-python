module Api::V1  
  class TokenController < Api::V1::ApiController
    skip_before_action  :verify_authenticity_token, :authenticate, raise: false

    include TokenHelper

    def create
      raise_status = :unauthorized
      message = nil
      error_code = nil

      1.times do
        break if !valid_request_origin?
        auth = request.headers['Authorization']
        raise Api::V1::ApiError.new(:bad_request, "Invalid header format") if (auth.blank? || auth.split.size < 2)

        type, token = auth.split(' ', 2)

        begin
          if (params[:g_token])
            raise Api::V1::ApiError.new(:not_found) if (params[:refresh])
            @token, api_user, api_org = issue_token_from_g_token(type, token, request)

            Current.set_user(api_user)
            Current.set_org(api_org)
          else
            @token, api_user, api_org = params[:refresh] ? 
              refresh_token(*auth.split, logger) : issue_token(type, token, request, logger)

            Current.set_user(api_user)
            Current.set_org(api_org)
          end
        rescue Api::V1::ApiError => e
          @token = nil
          raise_status = e.status if (e.respond_to?(:status))
          if e.respond_to?(:response) && !e.response.blank?
            message = e.response[:message]
            error_code = e.response[:error_code]
          end
        rescue => e
          logger.info("EXCEPTION: TokenController: status: #{e.message}, #{e.backtrace}")
          @token = nil
        end

        break if @token.blank?

        validate_account(current_user, current_org)
        @api_org_membership = OrgMembership.where(:org => current_org, :user => current_user)[0] if !current_org.nil?
        audit_user_login @token
        @admin_access = can?(:manage, Org)
        render "show_token"
        return
      end

      raise Api::V1::ApiError.new(raise_status, message, error_code)
    end

    def create_idp
      raise_status = :unauthorized
      message = nil
      error_code = nil

      auth_config = ApiAuthConfig.find_by_uid(params[:uid])
      if (auth_config.nil?)
        head :not_found
        return
      end

      1.times do
        begin
          case auth_config.protocol
          when ApiAuthConfig::Protocols[:saml]
            response = OneLogin::RubySaml::Response.new(params[:SAMLResponse],
              :settings => auth_config.get_assertion_settings)
            if (response.is_valid?)
              @token, api_user, api_org = 
                issue_token_from_saml(response.nameid, response.attributes, auth_config, request)

              Current.set_user(api_user)
              Current.set_org(api_org)

              @api_org_membership = OrgMembership.where(:org => current_org, :user => current_user)[0] if !current_org.nil?
              render "show" and return
            else
              err_msg = {
                :uid => auth_config.uid,
                :errors => response.errors
              }.to_json
              raise Api::V1::ApiError.new(:bad_request, err_msg)
            end
          when ApiAuthConfig::Protocols[:oidc]
            @token, api_user, api_org =
              issue_token_from_oidc(request, auth_config, params[:id_token], params[:access_token])

            Current.set_user(api_user)
            Current.set_org(api_org)

            @api_org_membership = OrgMembership.where(:org => current_org, :user => current_user)[0] if !current_org.nil?
            render "show" and return
          when ApiAuthConfig::Protocols[:google]
            break if !valid_request_origin?
            auth = request.headers['Authorization']
            @token, api_user, api_org = issue_token_from_g_token(*auth.split, request)

            Current.set_user(api_user)
            Current.set_org(api_org)
          when ApiAuthConfig::Protocols[:password]
            break if !valid_request_origin?
            auth = request.headers['Authorization']
            @token, api_user, api_org = params[:refresh] ?
              refresh_token(*auth.split, logger) : issue_token(*auth.split, request, logger)

            Current.set_user(api_user)
            Current.set_org(api_org)
          else
            raise Api::V1::ApiError.new(:bad_request)
          end
        rescue Api::V1::ApiError => e
          logger.info("EXCEPTION: TokenController ApiError: status: #{e.status}, #{e.backtrace}")
          @token = nil
          raise_status = e.status if (e.respond_to?(:status))
          if e.respond_to?(:response) && !e.response.blank?
            message = e.response[:message]
            error_code = e.response[:error_code]
          end
        rescue => e
          logger.info("EXCEPTION: TokenController: status: #{e.message}, #{e.backtrace}")
          @token = nil
        end

        break if @token.blank?

        validate_account(current_user, current_org)
        @api_org_membership = OrgMembership.where(:org => current_org, :user => current_user)[0] if !current_org.nil?
        audit_user_login @token
        render "show_token" and return
      end

      raise Api::V1::ApiError.new(raise_status, message, error_code)
    end

    def invalidate
      auth = request.headers['Authorization'].split
      raise Api::V1::ApiError.new(:bad_request, "Invalid header format") if (auth.size != 2)
      invalidate_access_token(*auth, request, logger)
      audit_user_logout(auth[1])
      head :ok
    end

    def aws_marketplace_token
      raise Api::V1::ApiError.new(:bad_request) if params[:token].blank?
      render :json => AwsService.exchange_marketplace_token(params[:token])
    end
  end
end

