module Common
  module Search
    class SearchWithElastic
      AND_CLAUSE = 'and'.freeze
      OR_CLAUSE = 'or'.freeze

      CLAUSES = [AND_CLAUSE, OR_CLAUSE].freeze

      FILTERS_TO_REMOVE = ['resource_table_name'].freeze
      NO_ARG_OPERATORS = ['empty', 'not_empty'].freeze
      SUPPORTED_OPERATORS = ['eq', 'not_eq', 'gt', 'gte', 'lt', 'lte', 'empty', 'not_empty', 'contains', 'not_contains', 'starts_with' ,'ends_with'].freeze

      # @param model [Class] model class to search. For flow search use synthetic class FlowSearch.
      def initialize(user: ,model:, filters:, tables: nil, org: nil, search_service_instance: SearchService::OrgSearch.new(org))
        raise ArgumentError.new("Search service instance is required") unless search_service_instance.is_a?(SearchService::BaseSearch)

        @user = user
        @org = org
        @model = model
        @filters = filters.is_a?(Array) ? filters.dup : [filters]
        @tables = tables || @model.table_name
        @search_service_instance = search_service_instance
      end

      def call
        filters.compact! if filters.is_a?(Array)
        if filters.blank? || filters == [AND_CLAUSE]
          Rails.configuration.x.error_logger.warn("No filters provided for search, model=#{model}")
          return []
        end

        filters.unshift(AND_CLAUSE) unless filters[0].is_a?(String)

        params = prepare_filters(filters)

        query = create_query_from_params(params)
        query = build_table_query(query)

        es_query = Common::Search::ElasticSearchQueryBuilder.new(query, model: model, tables: tables).call

        response = search_service_instance.execute_query(es_query)
        result = response.dig("hits", "hits")

        warn_max_hits(result.size)
        result
      end

      private
      attr_reader :user, :org, :model, :filters, :fields, :tables, :search_service_instance

      def prepare_filters(input_filters)
        Array.wrap(input_filters).map do |filter|
          case filter
          when ActionController::Parameters
            filter.to_unsafe_h.with_indifferent_access
          when Array
            prepare_filters(filter)
          when Hash
            filter.with_indifferent_access
          else
            filter
          end
        end.tap do |filters|
          filters.unshift(AND_CLAUSE) if filters.length == 1
        end.reject do |row|
          operator = row.is_a?(Hash) ? row['operator'] : nil
          if operator && !SUPPORTED_OPERATORS.include?(operator)
            raise ArgumentError.new("Invalid operator '#{operator}'")
          end
          row.is_a?(Hash) && ( FILTERS_TO_REMOVE.include?(row['field']) || (!NO_ARG_OPERATORS.include?(operator) && row['value'].nil?) )
        end
      end

      def create_query_from_params(filters)
        operator = filters.shift

        unless operator.is_a?(String) && CLAUSES.include?(operator.downcase)
          # TODO: Keep Api errors in controllers. Here we should throw `ArgumentError` or some custom Exception
          # that will be documented and as possible side effect of this class
          raise Api::V1::ApiError.new(:bad_request, "Invalid #{operator} filter clause")
        end
        operator = operator.downcase.to_sym

        es_filter = filters.map do |filter|
          filter.is_a?(Array) ? create_query_from_params(filter) : specify_for_table(filter)
        end

        { operator => es_filter }
      end

      def specify_for_table(filter)
        return filter if model != FlowSearch

        model_class = model_for_field(filter[:field])

        return filter if model_class.blank?

        filter[:field] = 'id'
        { and: [filter, { field: SearchService::BaseSearch::TABLE_NAME_FIELD, operator: "eq", value: model_class.table_name }] }
      end

      # Returns associated model with field name passed
      #
      # @param field [String] name of the field passed in filter
      # @return [ApplicationRecord, nil]
      def model_for_field(field)
        case field
        when 'data_source_id' then DataSource
        when 'data_set_id' then DataSet
        end
      end

      def build_table_query(main_query)
        {and: [{ field: SearchService::BaseSearch::TABLE_NAME_FIELD, value: tables, operator: "eq"}, main_query] }
      end

      def warn_max_hits(hits_size)
        limit = Common::Search::ElasticSearchQueryBuilder::RESULTS_SIZE_LIMIT
        if hits_size >= limit
          Rails.logger.warn("Search results have exceeded the maximum number of records (#{limit}). User: #{user.email}, Org: #{org&.id}, Model: #{model.name}, Filters: #{filters}")
        end
      end
    end
  end
end
