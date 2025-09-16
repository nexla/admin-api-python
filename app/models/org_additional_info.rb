class OrgAdditionalInfoValidator < ActiveModel::Validator

  def validate(record)
    data = record.data.to_h
    data.each_key do |key|
      next if data[key].blank?
      record.errors.add :base, "#{key.humanize} has invalid format." unless data[key] =~ SelfSignupRequest::ADDITIONAL_INFO_FORMAT
    end
  end
end

class OrgAdditionalInfo < ApplicationRecord

  validates_with OrgAdditionalInfoValidator

  belongs_to :org
  belongs_to :self_signup_request

  include JsonAccessor

  json_accessor :data
end
