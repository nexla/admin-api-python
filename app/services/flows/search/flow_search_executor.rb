module Flows
  module Search
    class FlowSearchExecutor

      def initialize(user, org, filters, access_role = :all, project_id = nil, sort_opts={})
        @user = user
        @org = org
        @filters = filters
        @access_role = access_role
        @project_id = project_id
        @sort_opts = sort_opts
      end

      def call
        if search_by_sql?
          search_results = search_by_sql
        else
          add_filter({field: 'project_id', value: project_id, operator: 'eq'}) if project_id
          add_filter({field: 'owner_id', value: user.id, operator: 'eq'})      if access_role == :owner_only

          replace_status_filter!(filters)

          search_results = Common::Search::SearchWithElastic
                      .new(user: user, org: org, model: FlowSearch, filters: filters, tables: FlowSearch::SEARCH_FLOW_TABLES)
                      .call
        end

        resources_hash = build_resources_hash(search_results)
        Flows::Builders::FromSearchedResources.new(resources_hash, user, org, access_role, project_id, sort_opts).call
      end

      def add_filter(filter)
        filters = self.filters

        includes_or = filters.is_a?(Array) && filters.any?{|v| v.is_a?(String) && v.downcase == 'or' }
        if filters.is_a?(Array) && !includes_or
          filters << filter
        else
          filters = [ 'and', filter, filters ]
        end

        self.filters = filters
      end

      private

      attr_reader :user, :org, :access_role, :project_id, :sort_opts
      attr_accessor :filters

      def build_resources_hash(results)
        resources = {
          data_source_ids: [],
          data_sink_ids: [],
          data_set_ids: [],
          data_credential_ids: [],
          code_containers_ids: [],
          flow_node_ids: []
        }
        results.each do |row|
          table_name = row["_source"][SearchService::BaseSearch::TABLE_NAME_FIELD]
          res_id = "#{table_name.singularize}_ids".to_sym

          resources[res_id].push(row["_source"]["id"])
        end
        resources
      end

      def search_by_sql?
        return false unless filters.is_a?(Array)

        has_flow_type = filters.any?{|f| f && !f.is_a?(String) && !f.is_a?(Array) &&  f[:field] == 'flow_type' && f[:operator] == 'eq' }
        has_project_id = filters.any?{|f| f && !f.is_a?(String) && !f.is_a?(Array) && f[:field] == 'project_id'  && f[:operator] == 'eq' }

        return true if has_flow_type && has_project_id && filters.length == 2

        has_flow_type && filters.size == 1
      end

      def search_by_sql
        flow_type = filters.detect{|f| f[:field] == 'flow_type' }

        scope = FlowNode.origins_only.where(org_id: org.id)
        scope = scope.where(flow_type: flow_type[:value])
        scope = scope.where(project_id: project_id) if project_id

        scope.pluck(:id).map do |id|
          {
            "_source" => {
              SearchService::BaseSearch::TABLE_NAME_FIELD => FlowNode.table_name,
              "id" => id
            }
          }
        end
      end

      def replace_status_filter!(filters)
        filters.each_with_index do |filter, index|
          if params_hash?(filter) && filter[:field] == 'status' && filter[:operator] == 'eq'
            filters[index] = ['and', filter, {field: :is_origin, value: true, operator: 'eq'}]
          elsif filter.is_a?(Array)
            replace_status_filter!(filter)
          end
        end
      end

      def params_hash?(value)
        value.is_a?(Hash) || value.is_a?(ActionController::Parameters)
      end

    end
  end
end