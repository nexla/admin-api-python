module Common
  module Search
    class ElasticSearchQueryBuilder

      # Unused
      DATA_FLOW_SEARCH_TABLES = [
        DataSource.table_name, DataSink.table_name, DataSet.table_name, DataCredentials.table_name
      ].freeze

      NON_STRING_TYPES = [:integer, :boolean, :datetime, :date, :float].freeze

      PREDEFINED_FIELDS = ['resource_table_name', 'tags'].freeze

      BOOLEAN_TRUE_VALUES = ['true', 't', '1', 'yes', 'y'].freeze

      NUMERIC_ONLY_OPERATORS = ['gt', 'gte', 'lt', 'lte'].freeze

      STRING_ONLY_OPERATORS = ['contains', 'not_contains', 'starts_with', 'ends_with'].freeze

      VENDOR_FIELDS = ['connector_code', 'connector_name', 'sink_connector_code', 'sink_connector_name', 'source_connector_code', 'source_connector_name'].freeze

      FLOW_SEARCH_MODELS = [DataSource, DataSet, DataSink, DataCredentials, CodeContainer, DataMap, DataSchema, DocContainer, Project, FlowNode]

      SEARCH_FIELDS = ['id', SearchService::BaseSearch::TABLE_NAME_FIELD].freeze

      RESULTS_SIZE_LIMIT = 10000

      def initialize(where, model: nil, tables: nil )
        @where = where
        @model = model
        @tables = Array(tables)
      end

      def call
        {
          size: RESULTS_SIZE_LIMIT,
          query: {
            bool: {
              must: {
                match_all: {}
              }
            }
          }
        }.tap do |body|
          body[:_source] = SEARCH_FIELDS
          body[:query][:bool][:filter] = build(where) if where.present?
        end
      end

      private
      attr_reader :where, :size, :tables, :model

      def build(where)
        filters = []
        if where[:or].present?
          filters << { bool: {should: where[:or].map { |or_statement| {bool: {filter: build(or_statement)}} }} }
        elsif where[:and].present?
          filters << { bool: {must: where[:and].map { |and_statement| {bool: {filter: build(and_statement)}} }} }
        else
          filters << field_condition(where[:field],where[:value],where[:operator])
        end
        filters
      end

      def field_condition(field, value, operator = "eq")
        validate_model_field(field)
        validate_field_operator(field, operator, value)

        preprocessed_field = preprocess_vendor(field, operator, value)
        if preprocessed_field
          field, operator, value = preprocessed_field
        end

        case operator.to_s
        when 'eq'
          return {terms: {field => value}} if value.is_a?(Array)
          {term: {field => {value: typed_value(field, value)}}}
        when 'not_eq'
          return {bool: { must_not: [{ terms: {field => value} }] }} if value.is_a?(Array)
          {bool: { must_not: [{ term: {field => {value: typed_value(field, value)}} }] }}
        when 'empty'
          construct_empty_filter(field)
        when 'not_empty'
          construct_non_empty_filter(field)

        when 'gt'
          { range: {field => { gt: value }}}
        when 'gte'
          { range: {field => { gte: value }}}
        when 'lt'
          { range: {field => { lt: value }}}
        when 'lte'
          { range: {field => { lte: value }}}

        when 'contains'
          {regexp: {field => {value: ".*#{ escape(value) }.*", flags: "NONE", case_insensitive: true}}}
        when 'not_contains'
          {bool: { must_not: [{regexp: {field => {value: ".*#{ escape(value) }.*", flags: "NONE", case_insensitive: true}}}] } }
        when 'starts_with'
          {regexp: {field => {value: "#{ escape(value) }.*", flags: "NONE", case_insensitive: true}}}
        when 'ends_with'
          {regexp: {field => {value: ".*#{ escape(value) }", flags: "NONE", case_insensitive: true}}}
        else
          raise ArgumentError.new("Invalid filter operator '#{operator}'")
        end
      end

      def preprocess_vendor(field, operator, value)
        return nil unless field.in?(VENDOR_FIELDS) && operator == 'eq'

        [field, 'contains', "::#{value}::"]
      end

      def typed_value(field, value)
        if boolean_field?(field)
          value = BOOLEAN_TRUE_VALUES.include?(value.to_s.downcase)
        elsif string_field?(field)
          value = value.to_s.downcase
        elsif field == "id"
          value = value.to_i
        end
        return value
      end

      def validate_model_field(field)
        return if field.in?(PREDEFINED_FIELDS)
        return if model.searchable_attributes_names.include?(field.to_s)
        raise ArgumentError.new("Attribute '#{field}' doesn't exist in #{model_name}")
      end

      def validate_field_operator(field, operator, value)
        if STRING_ONLY_OPERATORS.include?(operator)
          return validate_string_operator(operator, field)
        end

        if NUMERIC_ONLY_OPERATORS.include?(operator)
          validate_numeric_operator(operator, field)
        end

        if operator == 'eq' && numeric_field?(field) && !numeric_value?(value)
          raise ArgumentError.new("Invalid value '#{value}' for numeric field '#{field}'")
        end
      end

      def validate_string_operator(operator, field)
        return if string_field?(field)
        raise ArgumentError.new("Invalid filter operator '#{operator}' for non-string field '#{field}'")
      end

      def validate_numeric_operator(operator, field)
        raise ArgumentError.new("Invalid filter operator '#{operator}' for string field '#{field}'") if string_field?(field)
        raise ArgumentError.new("Invalid filter operator '#{operator}' for string field '#{field}'") if boolean_field?(field)
      end

      def construct_empty_filter(field)
        universal_condition = {
          "bool": {
            "must_not": {
              "exists": { "field": field }
            }
          }
        }

        return universal_condition unless string_field?(field)

        {
          bool: {
            should: [
              { match: { field => "" }},
              universal_condition
            ],
            minimum_should_match: 1
          }
        }
      end

      def construct_non_empty_filter(field)
        universal_condition = {
          bool: {
            must: [
              { exists: { field: field }}
            ]
          }
        }

        return universal_condition unless string_field?(field)

        universal_condition[:bool][:must] << { bool: { must_not: { match: { field => "" }}}}
        universal_condition
      end

      def escape(str)
        Regexp.escape(str.to_s).gsub('"', '\"')
      end

      def string_field?(field)
        types = models.map { |model| model.columns_hash[field]&.type }

        not_a_string = types.any?{|t| t.in?(NON_STRING_TYPES) }
        return false if not_a_string

        true
      end

      def boolean_field?(field)
        types = models.map { |model| model.columns_hash[field]&.type }
        types.any?{|t| t == :boolean }
      end

      def numeric_field?(field)
        types = models.map { |model| model.columns_hash[field]&.type }
        types.any?{|t| t.in?([:integer, :float]) }
      end

      def numeric_value?(value)
        value.is_a?(Numeric) || value.to_s.gsub(/\s/,'').match?(/\A[-+]?\d+\.?\d*\z/)
      end

      def models
        @models ||= tables.map { |table| Common::ResourceInflator.class_by_resource_name(table) }
      end

      def model_name
        if model.name.in?(['FlowSearch', 'FlowNode'])
          "a flow"
        else
          model.name.underscore.humanize.downcase.pluralize
        end
      end
    end
  end
end
