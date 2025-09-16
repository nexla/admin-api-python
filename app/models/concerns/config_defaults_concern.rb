module ConfigDefaultsConcern
  extend ActiveSupport::Concern

  def apply_config_defaults (api_user_info)
    type_name_prefix = self.class.name.underscore.gsub("data_", "")
    user_settings_type = UserSettingsType.find_by(name: type_name_prefix + "_config_defaults")
    return unless user_settings_type.present?

    config_defaults = api_user_info.input_owner.user_settings
      .where(org: api_user_info.input_org, user_settings_type_id: user_settings_type.id)
      .first

    if config_defaults.present? && config_defaults.settings[self.connector.type].is_a?(Hash)
      config_set_method = (type_name_prefix + "_config=").to_sym
      config_get_method = (type_name_prefix + "_config").to_sym
      self.send(config_set_method,
        config_defaults.settings[self.connector.type].merge(self.send(config_get_method))
      )
    end
  end
end