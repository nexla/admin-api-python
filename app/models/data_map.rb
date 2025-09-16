class DataMap < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include Api::V1::Schema
  include JsonAccessor
  include AccessControls::Standard
  include Accessible
  include Summary
  include Docs
  include SearchableConcern
  include ReferencedResourcesConcern
  include Copy

  belongs_to     :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to     :org
  belongs_to     :data_sink
  belongs_to     :copied_from,
                  class_name: "DataMap",
                  foreign_key: "copied_from_id"

  before_save    :handle_before_save
  after_commit   :after_commit

  acts_as_taggable_on :tags
  def tags_list
    self.tags.pluck(:name)
  end
  alias_method :tag_list, :tags_list


  mark_as_referenced_resource

  json_array_accessor :data_map
  json_accessor :data_defaults, :map_entry_schema

  attr_accessor :skip_refresh
  after_initialize do
    self.skip_refresh = false
  end

  Schema_Sample_Size = 20

  def origin_node_ids
    ResourcesReference.origin_nodes_ids_for(self)
  end

  def origin_nodes
    FlowNode.where(id: self.origin_node_ids)
  end

  def self.build_from_input(api_user_input, input)
    data_map = DataMap.new
    data_map.update_mutable!(api_user_input, input)
    return data_map
  end

  def set_defaults (user, org)
    self.owner = user
    self.org = org
  end

  def update_mutable! (api_user_info, input)
    return if input.nil?

    tags = input.delete(:tags)

    # :name must be non-empty, :description can be empty
    self.name         = input[:name] if !input[:name].blank?
    self.description  = input[:description] if input.key?(:description)

    if (input.key?(:public))
      raise Api::V1::ApiError.new(:method_not_allowed, "Input cannot include public attribute") if !api_user_info.user.super_user?
      self.public = !!input[:public]
    end

    self.data_type             = input[:data_type] if !input[:data_type].blank?
    self.data_format           = input[:data_format] if input.key?(:data_format)
    self.emit_data_default     = input[:emit_data_default] if input.key?(:emit_data_default)
    self.map_primary_key       = input[:map_primary_key] if input.key?(:map_primary_key)
    self.use_versioning        = input[:use_versioning] if input.key?(:use_versioning)

    self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
    self.org =   api_user_info.input_org   if (self.org != api_user_info.input_org)

    if (input.key?(:data_sink_id))
      ability = Ability.new(self.owner)
      dsink = DataSink.find(input[:data_sink_id])
      raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data set") if !ability.can?(:read, dsink)
      self.data_sink_id = dsink.id
      # We don't support creating dynamic data maps with versioning
      # turned on. The reasons for this are buried somewhere in NEX-4018.
      # But we do allow a subsequent PUT to set use_versioning == 1,
      # which is NEX-9276. Use with caution.
      self.use_versioning = false
      self.data_map = nil
    end

    if input.key?(:data_defaults)
      self.data_defaults = nil
      if input[:data_defaults].is_a?(String)
        self.data_defaults = { :key => "unknown", :value => input[:data_defaults] }
      elsif input[:data_defaults].is_a?(Hash)
        self.data_defaults = input[:data_defaults]
      end
    end

    if (input.key?(:data_map) && self.data_sink_id.nil?)
      self.data_map = nil
      if input[:data_map].is_a?(Hash)
        tmp = []
        input[:data_map].each do |k, v|
          tmp << { :key => k, :value => v }
        end
        self.data_map = tmp
        self.map_primary_key = "key"
      elsif input[:data_map].is_a?(Array)
        self.data_map = input[:data_map]
      end

      validate_primary_key_presence!(self.data_map) if self.static_data_map?
    end

    if self.static_data_map? && self.map_primary_key.blank?
      raise Api::V1::ApiError.new(:bad_request, "Map should have a primary key")
    end

    self.save!
    ResourceTagging.add_owned_tags(self, {tags: tags}, api_user_info.input_owner)
  end

  def validate_primary_key_uniqueness!(entries)
    set = Set.new
    entries.each do |row|
      pk_value = row[self.map_primary_key]
      if set.include?( pk_value )
        raise Api::V1::ApiError.new(:bad_request, "Primary key should have unique values")
      end
      set.add(pk_value)
    end
  end

  def validate_primary_key_presence!(entries)
    if entries.any? { |row| row[self.map_primary_key].blank? }
      raise Api::V1::ApiError.new(:bad_request, "DataMap entries primary key should not be empty")
    end
  end

  def set_map_entries(input)
    entries = input[:entries] || input[:data_map]
    raise Api::V1::ApiError.new(:bad_request, "No valid entries in request") if entries.blank?

    if (entries.is_a?(Hash))
      tmp = []
      entries.each do |k, v|
        tmp << (v.is_a?(Hash) ? v : { "key" => k, "value" => v })
      end
      entries = tmp
    end

    entries.each do |row|
      row.transform_values!(&:to_s)
    end

    if self.static_data_map?
      current_map = self.data_map
      validate_primary_key_presence!(entries)
      validate_primary_key_uniqueness!(entries)

      entries.each do |entry|
        current_map.delete_if { |e| e[self.map_primary_key] == entry[self.map_primary_key] }
      end
      entries.each do |entry|
        current_map << entry
      end

      self.skip_refresh = true
      self.data_map = current_map
      self.save!
    end
    return TransformService.new.set_map_entries(self, entries)
  end

  def delete_map_entries (keys, use_post: true)
    raise Api::V1::ApiError.new(:bad_request, "Invalid entry key format") unless keys.is_a?(String) || keys.is_a?(Array)

    if keys.is_a?(String)
      keys = keys.sub(/^['"]/, '').sub(/['"]$/, '')
      key_list = keys.split(",").map(&:strip)
    else
      key_list = keys
    end
    raise Api::V1::ApiError.new(:bad_request, "Invalid entry keys") if key_list.blank?

    key_list = key_list.map {|k| k.respond_to?(:strip) ? k.strip : k }

    if self.static_data_map?
      updated_map = self.data_map
      key_list.each do |key|
        updated_map.delete_if {|e|  e[self.map_primary_key].to_s.strip == key.to_s }
      end
      self.skip_refresh = true
      self.data_map = updated_map
      self.save!
    end

    if use_post
      return TransformService.new.delete_map_entries_in_body(self, key_list)
    else
      return TransformService.new.delete_map_entries(self, key_list)
    end
  end

  def get_map_validation
    result = {
      :cached => false,
      :cached_entry_count => 0,
      :static_entry_count => 0
    }
    result[:static_entry_count] = self.data_map.size if self.static_data_map?
    validation = TransformService.new.validate_data_map(self)
    ok = (validation[:status] == 200) || (validation[:status] == :ok)
    if (ok && !validation[:output].blank?)
      result[:cached] = !!validation[:output]["valid"]
      result[:cached_entry_count] = ok ? validation[:output]["currentVersionEntryCount"].to_i : 0
    end
    result
  end

  def static_data_map?
    self.data_sink_id.nil?
  end

  def dynamic_data_map?
    !self.data_sink.nil?
  end

  def get_map_entry_schema
    if self.dynamic_data_map?
      return self.try(:data_sink).try(:data_set).try(:output_schema)
    elsif self.data_map.empty?
      return nil
    else
      return !self.map_entry_schema.empty? ? self.map_entry_schema :
        TransformService.new.accumulate_schema(self.data_map[0..100], self.org)
    end
  end

  def data_set
    self.try(:data_sink).try(:data_set)
  end
  
  def get_map_entry_count (get_dynamic_count = false)
    count = self.map_entry_count
    # Here we correct for counts that may not have been set
    # and saved yet in :handle_before_save
    if self.static_data_map?
      return count.nil? ? self.data_map.size : count
    else
      # List views should pass get_dynamic_count == false as the
      # resulting transform-http request for every map can quickly 
      # make the overall request take too long. Individual map views
      # should pass true to allow count to be shown in detailed views.
      return (get_dynamic_count ? get_map_validation[:cached_entry_count].to_i : 0)
    end
  end

  def encrypted_credentials
    {
      credsEnc: "",
      credsEncIv: "",
      credsId: 1
    }
  end

  def apply_default_values(data_map_sample)
    data_map_sample.map { |row| data_defaults.merge(row) }
  end

  def copy_pre_save(original_data_map, api_user_info, options)
    self.data_sink_id = nil
  end

  protected

  def handle_before_save
    if (self.static_data_map?)
      if (self.data_map.blank?)
        self.map_entry_count = 0
      else
        self.map_entry_count = self.data_map.size

        result = TransformService.new.accumulate_schema(
          apply_default_values(self.data_map.sample(Schema_Sample_Size)), self.org)

        if result[:status].blank? || ['200', 'ok'].include?(result[:status].to_s.downcase)
          self.map_entry_schema = result
        elsif !Rails.env.test?
          raise Api::V1::ApiError.new(result[:status], result)
        end
      end
    end
  end

  def after_commit
    unless self.skip_refresh
      TransformService.new.refresh_data_map(self)
    end
  end
end
