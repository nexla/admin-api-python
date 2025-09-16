module Api
  module V1
    class ServiceKeysController < Api::V1::ApiController

      before_action :find_service_key, only: [:destroy, :show, :update, :rotate, :pause, :activate]

      def index
        @service_keys = scope(params[:all].truthy?).page(@page).per_page(@per_page)
        set_link_header(@service_keys)
        render "index"
      end

      def show
        @show_dataplane_details = current_user.super_user?
        render "show"
      end

      def create
        authorize! :manage, current_user
        input = validate_body_json(ServiceKey).deep_symbolize_keys
        @service_key = ServiceKey.build_from_input(current_user, current_org, input)
        render "show"
      end

      def update
        authorize! :manage, current_user
        input = validate_body_json(ServiceKey).symbolize_keys
        api_user_info = ApiUserInfo.new(current_user, current_org, input)
        @service_key.update_mutable!(api_user_info, input)
        render "show"
      end

      def destroy
        authorize! :manage, current_user
        if @service_key.data_source.blank?
          @service_key.destroy
          head :ok
        else
          raise Api::V1::ApiError.new(:method_not_allowed, "Key cannot be deleted while associated to a Flow.")
        end
      end

      def rotate
        authorize! :manage, current_user
        @service_key.rotate!
        render 'show'
      end

      def pause
        authorize! :manage, current_user
        @service_key.pause!
        render 'show'
      end

      def activate
        authorize! :manage, current_user
        @service_key.activate!
        render 'show'
      end

      private

      def find_service_key
        @service_key = scope(true).where("id = ? or api_key = ?", params[:id], params[:id]).first
        raise Api::V1::ApiError.new(:not_found) if @service_key.nil?
      end

      def scope(all = false)
        if current_user.super_user? && all
          return ServiceKey.all
        end

        if current_org.has_admin_access?(current_user) && all
          return ServiceKey.where(org: current_org)
        end

        ServiceKey.where(owner: current_user, org: current_org)
      end

    end
  end
end
