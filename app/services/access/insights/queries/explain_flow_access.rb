module Access
  module Insights
    module Queries
      class ExplainFlowAccess
        def initialize(user, org, origin_nodes)
          @user = user
          @org = org
          @origin_nodes = origin_nodes
        end

        def call
          api_user_info = ApiUserInfo.new(@user, org)
          resources = gather_resources(api_user_info)

          format_result(resources)
        end

        private
        attr_reader :user, :org, :origin_nodes

        def gather_resources(api_user_info)
          resources = FlowNode.empty_flow
          resources.delete(:flow)

          origin_nodes.each do |origin_node|
            node_resources = origin_node.resources(api_user_info)

            resources.keys.each do |key|
              resources[key] += node_resources[key].to_a if node_resources[key].present?
            end
          end
          resources
        end

        def format_result(resources)
          result = {}

          resources.each do |key, value|
            next unless value.present?

            result[key] = value.map do |item|
              {
                id: item.id,
                name: item.name,
                description: item.description,
                access_role: item.get_access_role(user)
              }.compact
            end
          end

          result
        end
      end
    end
  end
end