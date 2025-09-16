module Access
  module Insights
    module Queries

      class ExplainMultipleResources

        def initialize(user, org, resources, resource_name)
          @user = user
          @resources = resources
          @resource_name = resource_name
          @org = org

          @resource_class = Common::ResourceInflator.class_by_resource_name(resource_name)
        end

        def call
          resource_id_field = Common::ResourceInflator.reference_field_name(resource_name)
          @resources.map do |resource|
            roles = resource.get_access_roles(user, org)
            if roles.present?
              {resource_id_field => resource.id, access_role: max_role(roles) }
            end
          end.compact
        end

        private
        attr_reader :user, :org, :resources, :resource_name, :resource_class

        def max_role(roles)
          return :owner if roles.include?(:owner)

          return nil unless roles.present?

          indexes = roles.map{|role| resource_class.access_roles.index(role) }
          max = indexes.min # less index is higher access

          resource_class.access_roles[max]
        end
      end
    end
  end
end