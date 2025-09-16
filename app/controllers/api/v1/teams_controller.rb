module Api::V1  
  class TeamsController < Api::V1::ApiController      
    include PaperTrailControllerInfo
    include DocsConcern
    include AccessorsConcern
    
    def index
      @teams = current_user.teams(current_org, access_role: request_access_role(:all))
        .page(@page).per_page(@per_page)
      set_link_header(@teams)
    end
    
    def show
      return if render_schema Team
      @team = Team.find(params[:id])
      authorize! :read, @team
    end
    
    def create
      input = (validate_body_json Team).symbolize_keys
      @team = Team.build_from_input(input, current_user, current_org)
      render "show"
    end
    
    def update
      input = (validate_body_json Team).symbolize_keys

      @team = Team.find_by_id(params[:id])
      raise Api::V1::ApiError.new(:not_found) if @team.nil?
      authorize! :manage, @team

      @team.update_mutable!(request, current_user, input)
      render "show"
    end
    
    def destroy
      @team = Team.find(params[:id])
      authorize! :manage, @team
      if (@team.has_acl_entries? && !params[:force].truthy?)
        raise Api::V1::ApiError.new(:method_not_allowed, "Include force=1 as a query parameter to delete team with active access permissions")
      end
      @team.destroy
      head :ok
    end
  end
end


