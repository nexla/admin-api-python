module Access
  module Insights
    module Queries

      ROLE_ORDER = ([:owner] + AccessControls::ALL_ROLES_SET - [:none]).freeze

      class UsersAccessToResourceInsights
        def initialize(resource)
          @resource = resource
          @resource_type = resource.class.name
        end

        def call
          @users_map = {}
          @teams_map = {}
          @orgs_map = {}
          @results = { users: [], teams: [], orgs: [] }

          resource_acls = resource.access_controls
          flow_acls = origin_nodes(resource).map(&:access_controls).flatten
          check_owner
          check_org_acl_records # org members of resource.org
          check_resource_user_acl_records(resource_acls)
          check_resource_team_acl_records(resource_acls)
          check_resource_org_acl_records(resource_acls) # resource.access_controls.where(accessor_type: 'ORG')

          check_flow_acl_records(flow_acls) unless resource_type == 'FlowNode'
          check_project_acl_records

          sort_results!
          results
        end

        private

        attr_reader :resource, :resource_type
        attr_accessor :results, :users_map, :teams_map, :orgs_map

        def origin_nodes(resource)
          return [resource.origin_node] if resource.respond_to?(:origin_node)
          return resource.origin_nodes if resource.respond_to?(:origin_nodes)
          []
        end

        def check_owner
          include_user(resource.owner_id, :owner, {resource_type: 'owner', resource_id: resource.owner_id},
                       'The user is the owner of the resource')
          if resource.org
            include_user(resource.org.owner_id, :owner, {resource_type: 'org', resource_id: resource.owner_id},
                         "The user is the owner of the organization that resource belongs to.")

            include_org(resource.org, :owner, {resource_type: 'org', resource_id: resource.org_id},
                        "The #{resource_word} belongs to the organization")
          end
        end

        def check_org_acl_records
          return unless resource.org

          active_member_ids = resource.org.org_memberships.active.pluck(:user_id)

          resource.org.access_controls.where(accessor_type: 'USER').each do |ac|
            role = ac.get_access_roles[0]
            next unless role
            next unless active_member_ids.include?(ac.accessor_id)

            description = if role == :admin
              "The user is an admin of the organization"
            else
              "The #{resource_word} belongs to the organization #{resource.org.name}, which the user has been granted access to"
            end

            include_user(ac.accessor_id, role, {resource_type: 'Org', resource_id: resource.org.id }, description)
          end
        end

        def check_resource_user_acl_records(resource_acls, type = :regular)

          description =  "The #{resource_word(type)} was shared directly with the user"

          resource_acls.select{|ac| ac.accessor_type == 'USER' }.each do |ac|
            role = ac.get_access_roles[0]
            next unless role

            include_user(ac.accessor_id, role, {resource_type: resource_type, resource_id: resource.id }, description)
          end
        end

        def check_resource_team_acl_records(resource_acls, type = :regular)
          resource_acls.filter{|ac| ac.accessor_type ==  'TEAM' }.each do |ac|
            team = ac.accessor

            role = ac.get_access_roles[0]
            next unless role

            user_description = "The user is part of the team #{team.name} which has access to the #{resource_word(type)}"
            team_description = "The #{resource_word(type)} was shared with team directly"

            team.members.each do |user|
              include_user(user.id, role, {resource_type: 'Team', resource_id: team.id }, user_description)
            end

            include_team(team, role, {resource_type: resource_type, resource_id: resource.id }, team_description)
          end
        end

        def check_resource_org_acl_records(resource_acls, type = :regular)
          resource_acls.filter{|ac| ac.accessor_type == 'ORG' }.each do |ac|
            org = ac.accessor

            role = ac.get_access_roles[0]
            next unless role

            user_description = "The user is part of the organization #{org.name} which has been granted access to the #{ resource_word(type) }"
            org_description = "The #{resource_word(type)} was shared directly with the organization #{org.name}"

            org.org_memberships.active.each do |om|
              include_user(om.user_id, role, {resource_type: 'Org', resource_id: org.id }, user_description)
            end

            include_org(org, role, {resource_type: resource_type, resource_id: resource.id }, org_description)
          end
        end

        def check_flow_acl_records(flow_acls)
          check_resource_user_acl_records(flow_acls, :flow)
          check_resource_org_acl_records(flow_acls, :flow)
          check_resource_team_acl_records(flow_acls, :flow)
        end

        def check_project_acl_records
          projects = origin_nodes(resource).map(&:project).compact.uniq
          return if projects.blank?

          project_acls = projects.map(&:access_controls).flatten
          check_resource_user_acl_records(project_acls, :project)
          check_resource_org_acl_records(project_acls, :project)
          check_resource_team_acl_records(project_acls, :project)
        end

        def include_user(user_id, role, access_reason, description)
          user = User.find_by(id: user_id)
          return unless user

          if (record = users_map[user_id])
            existing_index = ROLE_ORDER.index(record[:access_role])
            new_index = ROLE_ORDER.index(role.to_sym)
            if new_index > existing_index
              return
            else
              users_map.delete(user.id)
              results[:users].delete(record)
            end
          end


          record = {id: user.id, email: user.email || '-deleted-', access_role: role.to_sym,
                    access_reason: access_reason, description: description }
          results[:users] << record
          users_map[user.id] = record
        end

        def include_team(team, role, access_reason, description)
          if (record = teams_map[team.id])
            existing_index = ROLE_ORDER.index(record[:access_role])
            new_index = ROLE_ORDER.index(role.to_sym)
            if new_index > existing_index
              return
            else
              teams_map.delete(team.id)
              results[:teams].delete(record)
            end
          end
          record = {id: team.id, name: team.name, access_role: role.to_sym,
                    access_reason: access_reason, description: description }
          results[:teams] << record
          teams_map[team.id] = record
        end

        def include_org(org, role, access_reason, description)
          if (record = orgs_map[org.id])
            existing_index = ROLE_ORDER.index(record[:access_role])
            new_index = ROLE_ORDER.index(role.to_sym)
            if new_index > existing_index
              return
            else
              orgs_map.delete(org.id)
              results[:orgs].delete(record)
            end
          end

          record = {id: org.id, name: org.name, access_role: role.to_sym,
                    access_reason: access_reason, description: description }
          results[:orgs] << record
          orgs_map[org.id] = record
        end

        def sort_results!
          results[:users].sort_by!{|u| ROLE_ORDER.index(u[:access_role]) }
          results[:teams].sort_by!{|t| ROLE_ORDER.index(t[:access_role]) }
          results[:orgs].sort_by!{|o| ROLE_ORDER.index(o[:access_role]) }
        end

        def resource_word(type = :regular)
          return 'project' if type == :project
          return 'flow' if type == :flow

          resource_type == 'FlowNode' ? 'flow' : 'resource'
        end

      end
    end
  end
end
