class FlowLinkType < ApplicationRecord
  self.inheritance_column = :_type_disabled

  def self.load_from_config
    return if !ActiveRecord::Base.connection.table_exists?(FlowLinkType.table_name)
    specs = JSON.parse(File.read("#{Rails.root}/config/api/flow_link_types.json"))
    specs.each do |spec|
      next if FlowLinkType.find_by(type: spec["type"]).present?
      FlowLinkType.create(spec)
    end
  end

  def self.types
    FlowLinkType.all.pluck(:type, :type).map { |t| [t.first.downcase.to_sym, t.second] }.to_h
  end
end