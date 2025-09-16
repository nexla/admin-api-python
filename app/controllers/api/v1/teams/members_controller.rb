module Api::V1::Teams
  class MembersController < Api::V1::ApiController
    include PaperTrailControllerInfo

    def index
      @team = Team.find(params[:team_id])
      authorize! :read, @team
      @members = @team.members
      set_link_header(@members)
    end

    def edit
      input = (validate_body_json TeamMember).symbolize_keys

      @team = Team.find(params[:team_id])
      authorize! :manage, @team

      @team.update_members(input[:members], params[:mode].try(:to_sym))
      @members = @team.members
      set_link_header(@members)
      render "index"
    end
  end

end


