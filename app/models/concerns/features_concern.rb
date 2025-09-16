module FeaturesConcern
  extend ActiveSupport::Concern

  # To remove a feature from the list you need to find all of the occurrences in code of
  # method called `#{feature_name}_enabled?`. This could be automated if it's common problem
  # and if we do it often.
  class_methods do
    def defined_features
      %w[testing_feature another_feature marketplace].freeze
    end
  end

  included do
    defined_features.each do |feature_name|
      define_method "#{feature_name}_enabled?" do
        self.features_enabled ||= []
        features_enabled.include?(feature_name)
      end
    end
  end

  # Internally used method to enable features for given user
  def enable_feature!(feature_name)
    validate_feature_name(feature_name)
    self.features_enabled ||= []

    self.features_enabled << feature_name unless features_enabled.include?(feature_name)
    save!
  end

  # Internally used method to disable features for given user
  def disable_feature!(feature_name)
    validate_feature_name(feature_name)
    self.features_enabled ||= []

    self.features_enabled.delete(feature_name)
    save!
  end

  protected

  def validate_feature_name(feature_name)
    raise ArgumentError unless self.class.defined_features.include?(feature_name)
  end
end
