class OrgTier < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AuditLog
  include ThrottleConcern

  has_many :orgs

  Unlimited = -1

  def self.build_from_input (input)
    if (input[:name].nil? or input[:display_name].nil?)
      raise Api::V1::ApiError.new(:bad_request, "Invalid data input. name and display_name are required.")
    end

    org_tier = OrgTier.create(input)
    return org_tier

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

  def self.validate_data_source_activate (data_source)
    tier_resource = tier_resource(data_source)
    return true if tier_resource.nil?
    return true if tier_resource.data_source_count_limit < 0
    if (data_source.org_id.nil?)
      active_source_count = DataSource.where(
        owner_id: data_source.owner_id,
        org_id: nil,
        status: DataSource::Statuses[:active],
        data_sink_id: nil
      ).count
    else
      active_source_count = DataSource.where(
        org_id: data_source.org_id,
        status: DataSource::Statuses[:active],
        data_sink_id: nil
      ).count
    end
    return (active_source_count < tier_resource.data_source_count_limit)
  end

  def self.activate_rate_limited_sources(rate_limited_sources, data_source_count_limit, active_source_count = 0, resource = nil)
    if !rate_limited_sources.blank?
      activated_count = active_source_count
      rate_limited_sources.each do |source|
        break if (data_source_count_limit != OrgTier::Unlimited && activated_count >= data_source_count_limit)
        source.activate!
        activated_count = (activated_count + 1)
      end
    end
  end

  def self.pause_rate_limited_sources(active_sources, resource_count = 0, action = "rate_limited")
    if !active_sources.blank?
      if resource_count == 0
        active_sources.each do |source|
          action == "rate_limited" ? source.rate_limited! : source.pause!
        end
      else
        paused_count = 0
        active_sources.each do |source|
          break if (paused_count >= resource_count)
          action == "rate_limited" ? source.rate_limited! : source.pause!
          paused_count += 1
        end
      end
    end
  end

  def self.tier_resource(resource)
    if !resource.org.nil? && !resource.org.org_tier.nil?
      return resource.org.org_tier
    elsif !resource.owner.user_tier.nil?
      return resource.owner.user_tier
    else
      return nil
    end
  end

  def self.account_resource(resource)
    if !resource.org.nil? && !resource.org.org_tier.nil?
      return resource.org
    elsif !resource.owner.user_tier.nil?
      return resource.owner
    else
      return nil
    end
  end

  # Do not allow to throttle whole OrgTier.
  # It's part of the ThrottleConcern
  def throttled?
    false
  end

end
