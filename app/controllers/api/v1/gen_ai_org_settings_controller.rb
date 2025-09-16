module Api
  module V1
    class GenAiOrgSettingsController < ApiController
      def index
        if params[:all].present?
          require_nexla_admin!
          gen_ai_org_settings = GenAiOrgSetting.all
        elsif params[:org_id].present?
          require_nexla_admin!
          gen_ai_org_settings = GenAiOrgSetting.where(org_id: params[:org_id])
        else
          #TODO: hide Nexla's not global configs from it
          if current_org.nexla_admin_org?
            gen_ai_org_settings = GenAiOrgSetting.where(org_id: current_org.id)
          else
            gen_ai_org_settings = GenAiOrgSetting.joins(:gen_ai_config).where(org_id: current_org.id, global: false, gen_ai_configs: { org_id: current_org.id })
          end
        end

        @gen_ai_org_settings = gen_ai_org_settings.page(@page).per_page(@per_page)
        set_link_header(@gen_ai_org_settings)
      end

      def show
        @gen_ai_org_setting = GenAiOrgSetting.find(params[:id])
        authorize! :read, @gen_ai_org_setting.org
      end

      def active_config
        usage = params[:gen_ai_usage]
        @config = GenAiConfigProvider.new(current_org, usage).get_config
        if @config.nil?
          render json: { message: 'No active config found' }
        else
          render :active_config
        end
      end

      def create
        input = validate_body_json(GenAiOrgSetting)
        @gen_ai_org_setting = GenAiOrgSetting.build_from_input(input, ApiUserInfo.new(current_user, current_org))
        render :show
      end

      def destroy
        gen_ai_org_setting = GenAiOrgSetting.find(params[:id])
        authorize! :manage, gen_ai_org_setting.org
        gen_ai_org_setting.destroy

        head :ok
      end
    end
  end
end