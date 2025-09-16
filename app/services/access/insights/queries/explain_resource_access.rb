module Access
  module Insights
    module Queries
      class ExplainResourceAccess
        def initialize(user, resource)
          @user = user
          @resource = resource
          @rules = []
          @warn = nil
        end

        def call
          gather_origin_nodes

          check_owner
          check_superadmin
          check_admin_of_org

          check_user_acl_in_org_given

          resource_acls = resource.access_controls
          check_user_acl_given(resource_acls)
          check_team_acl_given(resource_acls)
          check_org_acl_given(resource_acls)
          check_flow_acl_given unless resource.is_a?(FlowNode)
          check_project_acl_given

          return format_response(@rules, @warn) if @rules.present?

          @rules = [{access_role: nil, description: 'No access'}]
          format_response(@rules, @warn)
        end

        private

        attr_reader :user, :resource

        attr_accessor :origin_nodes

        def gather_origin_nodes
          origin_node_ids = []
          if resource.respond_to?(:origin_node_id)
            origin_node_ids = [resource.origin_node_id]
          elsif resource.respond_to?(:origin_nodes)
            origin_node_ids = resource.origin_nodes.map(&:id)
          end
          self.origin_nodes = FlowNode.where(id: origin_node_ids.compact)
          if self.origin_nodes.blank?
            @warn = 'No origin nodes found for the resource'
          end
        end

        def check_owner
          if resource.owner_id == user.id
            @rules << {description: "The user is the owner of the #{resource_word}", access_role: 'owner'}
          end
        end

        def check_admin_of_org
          if !user.super_user? && resource.org&.has_admin_access?(user)
            @rules << {description: 'The user is an admin of the organization', access_role: 'admin'}
          end
        end

        def check_superadmin
          if user.super_user?
            @rules << {description: 'The user is a Nexla admin', access_role: 'admin'}
          end
        end

        def check_user_acl_given(acls, resource_descriptor = resource_word)
          ac = acls.where(accessor_type: 'USER', accessor_id: user.id).first
          if ac
            role = Access::RoleConvertor.int_to_sym(ac.role_index)
            @rules << {description: "The #{resource_descriptor} was shared directly with the user", access_role: role}
          end
        end

        def check_user_acl_in_org_given(resource_descriptor = resource_word)
          return unless resource.org

          ac = resource.org.access_controls.where(accessor_type: 'USER', accessor_id: user.id).first
          if ac
            role = Access::RoleConvertor.int_to_sym(ac.role_index)
            org_name = resource.org.name
            @rules << {description: "The #{resource_descriptor} belongs to the organization #{org_name}, which the user has been granted access to", access_role: role}
          end
        end

        def check_team_acl_given(acls, resource_descriptor = resource_word)
          team_ids = user.teams(:all, access_role: :all).pluck(:id)

          if team_ids && team_ids.size > 0
            team_acl = acls.where(accessor_type: 'TEAM', accessor_id: team_ids)
            return false unless team_acl.present?

            roles = []
            team_names = []
            team_acl.each do |team_ac|
              role = Access::RoleConvertor.int_to_sym(team_ac.role_index)
              roles << role
              team_names << Team.find(team_ac.accessor_id).name
            end
            roles = roles.flatten.uniq
            role, team_name = max_role(roles, team_names)
            if role
              @rules << { description: "The user is a part of the team #{team_name} which has been granted access to the #{resource_descriptor}", access_role: role}
            end
          end
        end

        def check_org_acl_given(acls, resource_descriptor = resource_word)
          org_ids = user.orgs.pluck(:id)
          if org_ids && org_ids.size > 0
            org_acl = acls.where(accessor_type: 'ORG', accessor_id: org_ids)
            return false unless org_acl.present?

            roles = []
            org_names = []
            org_acl.each do |org_ac|
              role = Access::RoleConvertor.int_to_sym(org_ac.role_index)
              roles << role
              org_names << Org.find(org_ac.accessor_id).name
            end
            roles = roles.flatten.uniq

            role, org_name = max_role(roles, org_names)
            if role
              @rules << { description: "The user is part of the organization #{org_name} which has been granted access to the #{resource_descriptor}", access_role: role}
            end
          end
        end

        def check_flow_acl_given
          origin_nodes.each do |fn|
            if fn.is_owner?(user)
              description = "The user is the owner of the flow that resource belongs to"
              @rules << { description: description, access_role: 'admin' }
            end
          end

          flow_acls = FlowNodesAccessControl.where(
            :flow_node_id => origin_nodes.map(&:id).uniq,
            :accessor_org_id => (resource.org&.id)
          )
          check_user_acl_given(flow_acls, "flow")
          check_team_acl_given(flow_acls, "flow")
          check_org_acl_given(flow_acls, "flow")
        end

        def check_project_acl_given
          projects = Project.where(id: origin_nodes.map(&:project_id).uniq)
          return if projects.blank?

          if projects.any?{|project| project.is_owner?(user) }
            @rules << { description: "The user is an owner of the project that #{resource_word} belongs to", access_role: 'admin' }
          end

          project_acls = ProjectsAccessControl.where(project: projects,
                                                     accessor_org_id: resource.org_id)
          check_user_acl_given(project_acls, "project")
          check_team_acl_given(project_acls, "project")
          check_org_acl_given(project_acls, "project")
        end

        def max_role(roles, items=nil)
          return nil unless roles.present?

          indexes = roles.map{|role| role && AccessControls::ALL_ROLES_SET.index(role) }
          max = indexes.compact.min # less index is higher access

          return nil unless max

          role = AccessControls::ALL_ROLES_SET[max]

          return role unless items.present?

          index = roles.index(role)
          [role, items[index]]
        end

        def format_response(rules, warn)
          result = { applicable_rules: rules }

          max_role = max_role_rule(rules)

          if rules.present? && max_role
            result[:effective_rule] = max_role
          else
            result[:effective_rule] = { access_role: nil, description: "No access" }
          end
          result[:warning] = warn if warn.present?
          result
        end

        def max_role_rule(rules)
          return nil unless rules.present?

          sorted = rules.each_with_index.sort_by do |rule, idx|
            role = rule[:access_role]&.to_sym
            next [999, 0] if role == :none || role == :member
            next [0, 0] if role == :owner

            role_idx = AccessControls::ALL_ROLES_SET.index(role)
            role_idx = role_idx.nil? || role_idx.zero? ? 999 : role_idx
            [role_idx, idx]
          end

          sorted.first[0] # array now consists of [rule, index] pairs, so we return rule only
        end

        def resource_word
          case
          when @resource.is_a?(FlowNode) then 'flow'
          when @resource.is_a?(Project) then 'project'
          else 'resource'
          end
        end
      end
    end
  end
end