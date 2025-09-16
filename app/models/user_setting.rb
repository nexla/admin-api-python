class UserSetting < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include JsonAccessor

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :user_settings_type

  json_accessor :settings

  def self.build_from_input (api_user_info, input)
    if (input[:user_settings_type].blank?)
      raise Api::V1::ApiError.new(:bad_request, "user_settings_type attribute must not be blank")
    end

    user_settings_type = UserSettingsType.find_by_name(input[:user_settings_type])
    if (user_settings_type.nil?)
      raise Api::V1::ApiError.new(:bad_request, "Invalid user_settings_type: #{input[:user_settings_type]}")
    end

    us = nil
    UserSetting.transaction do
      us = UserSetting.where(:owner => api_user_info.input_owner, :org => api_user_info.input_org,
        :user_settings_type_id => user_settings_type.id)
      us = us.empty? ? UserSetting.new : us.first
      us.owner = api_user_info.input_owner
      us.org = api_user_info.input_org
      us.user_settings_type = user_settings_type
      us.settings = {}
      us.update_mutable!(api_user_info, input, false)
    end

    raise Api::V1::ApiError.new(:internal_server_error) if us.nil?

    if !us.valid?
      status = us.status.nil? ? :bad_request : us.status.to_sym
      raise Api::V1::ApiError.new(status, us.errors.full_messages.join(";"))
    end

    return us
  end

  def update_mutable! (api_user_info, input, update_owner = true)
    if (update_owner)
      self.owner = api_user_info.input_owner
      self.org = api_user_info.input_org
    end
    self.description = input[:description] if input.key?(:description)
    pkey = self.user_settings_type.primary_key

    if (input[:settings].is_a?(Hash))
      s = self.settings
      input[:settings].each do |k, v|
        if (v.blank?)
          if v.nil?
            s.delete(k)
          else
            s[k] = v
          end

          if (!pkey.nil? && (k == pkey))
            self.primary_key_value = nil
          end
        else
          s[k] = v
          if (!pkey.nil? && (k == pkey))
            self.primary_key_value = v
          end
        end
      end
      self.settings = s
    end

    self.save!
  end

end
