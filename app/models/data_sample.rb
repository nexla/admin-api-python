class DataSample < ApplicationRecord
  self.primary_key = :id

  # Note, data_samples table is append ONLY. We never modify
  # existing records. 
  #
  # There is no separate _versions table: all new entries are
  # recorded as part of 'create' or 'update' events in data_set_versions.
  #
  # A data_set association is required. data_samples cannot be used for 
  # storing arbitrary sample data without a corresponding data_set_id. 
  #
  MAX_SAMPLES_COUNT = Rails.env.test? ? 5 : 30

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :data_set

  def self.create_for_data_set (data_set, samples)
    raise Api::V1::ApiError.new(:internal_server_error,
      "Cached samples must be an array!") if !samples.is_a?(Array)

    DataSample.create({
      owner_id: data_set.owner.id,
      org_id: data_set.org&.id,
      data_set_id: data_set.id,
      samples: DataSample.filter_samples(samples[0...MAX_SAMPLES_COUNT])
    })
  end

  def self.update_for_data_set (data_set, samples)
    previous_data_sample = DataSample.where(data_set_id: data_set.id).last
    DataSample.create(data_set, samples + (previous_data_sample&.samples || []))
  end

  def self.filter_samples (samples)
    # Note, here we serialize, filter, de-serialize then serialize
    # again in ActiveRecord. This might be a reason to go back to
    # using json_accessor with one-time (de-)serialization.
    JSON.parse(samples.to_json.gsub(EmojiRegex::Regex, ''))
  end

  def samples
    return [] if read_attribute(:samples).blank?
    super
  end
end