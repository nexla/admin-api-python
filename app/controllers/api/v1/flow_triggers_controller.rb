module Api::V1  
  class FlowTriggersController < Api::V1::ApiController
    include PaperTrailControllerInfo

    INDEX_ALL_MAX_PER_PAGE = 100
    pagination :all, per_page: INDEX_ALL_MAX_PER_PAGE, enforce: false

    def index
      @flow_triggers = FlowTrigger.accessible_by_user(current_user, current_org)
        .page(@page).per_page(@per_page)
      set_link_header(@flow_triggers)
    end

    def all
      head :forbidden and return if !current_user.super_user?
       @flow_triggers = FlowTrigger.all.page(@page).per_page(@per_page)
      set_link_header(@flow_triggers)
      render "index"
    end

    def show
      return if render_schema FlowTrigger
      @flow_trigger = FlowTrigger.find(params[:id])
      authorize! :operate, @flow_trigger
    end

    def create
      input = (validate_body_json FlowTrigger).symbolize_keys
      api_user_info = ApiUserInfo.new(current_user, current_org, input)
      @flow_trigger = FlowTrigger.build_from_input(api_user_info, input)
      render "show"
    end

    def update
      head :method_not_allowed
    end

    def destroy
      flow_trigger = FlowTrigger.find(params[:id])
      authorize! :manage, flow_trigger
      flow_trigger.destroy
      head :ok
    end

    def activate
      @flow_trigger = FlowTrigger.find(params[:flow_trigger_id])
      authorize! :operate, @flow_trigger
      if params[:activate]
        @flow_trigger.activate!
      else
        @flow_trigger.pause!
      end
      render "show"
    end

  end
end
