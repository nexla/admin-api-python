class UserSettingsType < ApplicationRecord
  self.primary_key = :id
  validates :name, :presence => true, :uniqueness => true

  def self.load_from_config
    return if !ActiveRecord::Base.connection.table_exists?(UserSettingsType.table_name)
    specs = JSON.parse(File.read("#{Rails.root}/config/api/user_settings_types.json"))
    specs.each do |spec|
      next if UserSettingsType.find_by(name: spec["name"]).present?
      UserSettingsType.create(spec)
    end
  end
end