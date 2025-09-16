class Connector < ApplicationRecord
  self.inheritance_column = :_type_disabled

  enum ingestion_mode: API_INGESTION_MODES

  scope :nexset_api_compatible, -> () { where(nexset_api_compatible: true) }

  def self.default_connection_type
    Connector.find_by_type("s3")
  end

  def self.load_connectors_from_config
    return if !ActiveRecord::Base.connection.table_exists?(Connector.table_name)
    connectors = JSON.parse(File.read("#{Rails.root}/config/api/connectors.json"))
    connectors.each do |c|
      next if Connector.find_by_type(c["type"]).present?
      Connector.create(c)
    end
  end

  def self.all_types_hash
    table_exists = ActiveRecord::Base.connection.table_exists?(Connector.table_name)
    if (!table_exists || Connector.all.empty?)
      connectors = JSON.parse(File.read("#{Rails.root}/config/api/connectors.json"))
      return connectors.map { |c| [c["type"].to_sym, c["type"]] }.to_h
    end
    return Connector.all.map { |c| [c.type.to_sym, c.type] }.to_h
  end

  # Can't use include Api::V1::Schema here because of load order
  def update_mutable!(input)
    unless Connector.ingestion_modes.values.include?( input[:ingestion_mode].to_s.downcase )
      raise Api::V1::ApiError.new(:bad_request, "Invalid ingestion_mode. Possible values: #{Connector.ingestion_modes.values.join(', ')}")
    end

    self.ingestion_mode = input[:ingestion_mode] if input.key?(:ingestion_mode)
    self.save!
  end
end
