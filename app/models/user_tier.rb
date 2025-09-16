class UserTier < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AuditLog

  has_many :users

  Unlimited = -1

  def self.build_from_input (input)
    if (input[:name].nil? or input[:display_name].nil?)
      raise Api::V1::ApiError.new(:bad_request, "Invalid data input. name and display_name are required.")
    end

    user_tier = UserTier.create(input)
    return user_tier
  end

  def update_mutable! (input)
    return if input.nil?

    self.name = input[:name] if !input[:name].blank?
    self.display_name = input[:display_name] if !input[:display_name].blank?
    self.record_count_limit = input[:record_count_limit] if !input[:record_count_limit].blank?
    self.record_count_limit_time = input[:record_count_limit_time] if !input[:record_count_limit_time].blank?
    self.data_source_count_limit = input[:data_source_count_limit] if !input[:data_source_count_limit].blank?
    self.save!
  end

end
