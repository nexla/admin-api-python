class FlowSearch

  SEARCH_FLOW_MODELS = [DataSource, DataSet, DataSink, DataCredentials, FlowNode].freeze
  SEARCH_FLOW_TABLES = SEARCH_FLOW_MODELS.map(&:table_name).freeze

  def self.searchable_attributes_names
    @searchable_attributes_names ||= SEARCH_FLOW_MODELS.map(&:searchable_attributes_names).flatten.uniq
  end

end