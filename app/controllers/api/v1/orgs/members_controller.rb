module Api::V1::Orgs
  class MembersController < Api::V1::ApiController

    include PaperTrailControllerInfo

    def index
      @org = Org.find(params[:org_id])
      authorize! :read, @org
      set_org_members
      render "api/v1/orgs/members"
    end

    def edit
      input = (validate_body_json OrgMember).symbolize_keys
      @org = Org.find(params[:org_id])
      authorize! :manage, @org
      @org.update_members(current_user, current_org, input[:members], params[:mode].try(:to_sym))
      set_org_members
      render "api/v1/orgs/members"
    end

    private

    def set_org_members
      @org_members, @org_access_roles, @org_roles_expirations = Org.org_users_with_roles(@org)

      set_link_header(@org_members)
    end
  end
end
  
  
  