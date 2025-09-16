class OrchestrationEventType < ApplicationRecord
  self.inheritance_column = :_type_disabled

  Types = API_ORCHESTRATION_EVENT_TYPES

  def self.load_from_config
    return if !ActiveRecord::Base.connection.table_exists?(OrchestrationEventType.table_name)
    specs = JSON.parse(File.read("#{Rails.root}/config/api/orchestration_event_types.json"))
    specs.each do |spec|
      next if OrchestrationEventType.find_by(type: spec["type"]).present?
      OrchestrationEventType.create(spec)
    end
  end
end