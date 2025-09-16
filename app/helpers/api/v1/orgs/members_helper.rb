module Api
  module V1
    module Orgs
      module MembersHelper
        def user_org_role(user, org)
          return :admin if org.has_admin_access?(user)
          return :admin_readonly if org.has_admin_readonly_access?(user)
          return :operator if org.has_operator_access?(user)
          return :collaborator if org.has_collaborator_access?(user)

          :member
        end
      end
    end
  end
end