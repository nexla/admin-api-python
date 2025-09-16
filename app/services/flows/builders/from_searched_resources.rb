module Flows
  module Builders
    class FromSearchedResources
      def initialize(resources_hash, user, org, access_role = :all, project_id=nil, sort_opts={})
        @resources = resources_hash
        @access_role = access_role
        @user = user
        @org = org
        @project_id = project_id
        @sort_opts = sort_opts
      end

      def call
        origin_node_ids = gather_origins
        apply_order( filter_accessible(origin_node_ids) )
      end

      private
      attr_reader :resources, :user, :org, :access_role, :project_id, :sort_opts

      def apply_order(scope)
        sort_field = sort_opts[:sort_by].try(:downcase) || 'created_at'
        sort_order = sort_opts[:sort_order].try(:downcase) || 'desc'

        unless FlowNode.column_names.include?(sort_field)
          raise ArgumentError.new("Invalid sort field: #{sort_field}")
        end

        scope.order(sort_field => sort_order)
      end

      def gather_origins
        resources[:data_source_ids] ||= []
        resources[:data_set_ids] ||= []

        if resources[:data_credentials_ids].present?
          resources[:data_source_ids] +=
            DataSource.where(data_credentials_id: resources[:data_credential_ids])
                      .pluck(:id)

          DataSink.where(data_credentials_id: resources[:data_credential_ids]).each do |d|
            fo = d.origin_node.resource
            if fo.is_a?(DataSource)
              resources[:data_source_ids] << fo.id
            elsif fo.is_a?(DataSet)
              resources[:data_set_ids] << fo.id
            end
          end
        end

        sources = resources[:data_source_ids].present? ? DataSource.where(id: resources[:data_source_ids]) : DataSource.none
        sets = resources[:data_set_ids].present? ? DataSet.where(id: resources[:data_set_ids]) : DataSet.none
        sinks = resources[:data_sink_ids].present? ? DataSink.where(id: resources[:data_sink_ids]) : DataSink.none
        flow_nodes = resources[:flow_node_ids].present? ? FlowNode.where(id: resources[:flow_node_ids]) : FlowNode.none

        origin_node_ids = []
        origin_node_ids += sources.pluck(:origin_node_id)
        origin_node_ids += sets.pluck(:origin_node_id)
        origin_node_ids += sinks.pluck(:origin_node_id)
        origin_node_ids += flow_nodes.pluck(:origin_node_id)

        if project_id.present?
          FlowNode.where(id: origin_node_ids, project_id: project_id).pluck(:id)
        else
          origin_node_ids
        end
      end

      def filter_accessible(origin_node_ids)
        FlowNode.accessible_by_user(user, org, access_role: self.access_role, selected_ids: origin_node_ids)
      end
    end
  end
end