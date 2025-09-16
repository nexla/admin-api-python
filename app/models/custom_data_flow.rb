class CustomDataFlow < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AccessControls::Standard
  include AuditLog
  include JsonAccessor
  include Copy
  include Docs
  include SearchableConcern

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :copied_from, class_name: "CustomDataFlow", foreign_key: "copied_from_id"

  has_many :custom_data_flows_code_containers,
    dependent: :destroy
  has_many :code_containers,
    through: :custom_data_flows_code_containers

  has_many :custom_data_flows_data_credentials,
    class_name: "CustomDataFlowsDataCredentials",
    dependent: :destroy
  has_many :data_credentials,
    class_name: "DataCredentials",
    source: :data_credentials,
    through: :custom_data_flows_data_credentials

  acts_as_taggable_on :tags
  def tags_list
    self.tags.pluck(:name)
  end
  alias_method :tag_list, :tags_list

  json_accessor :config

  Flow_Types = API_CUSTOM_DATA_FLOW_TYPES
  Default_Flow_Type = Flow_Types[:airflow]

  Statuses = {
    :init          => 'INIT',
    :paused        => 'PAUSED',
    :active        => 'ACTIVE',
    :rate_limited  => 'RATE_LIMITED'
  }

  def self.flow_types_enum
    enum = "ENUM("
    first = true
    Flow_Types.each do |k, v|
      enum += "," if !first
      enum += "'#{v}'"
      first = false
    end
    enum + ")"
  end

  def self.validate_flow_type_str (flow_type_str)
    return nil if flow_type_str.class != String
    return nil if Flow_Types.find { |sym, str| str == flow_type_str }.nil?
    flow_type_str
  end

  def self.validate_status_str (status_str)
    return nil if status_str.class != String
    status_str = status_str.upcase
    return nil if Statuses.find { |sym, str| str == status_str }.nil?
    status_str
  end

  def self.build_from_input (api_user_info, input, request)
    if (input[:name].blank?)
      raise Api::V1::ApiError.new(:bad_request, "name attribute must not be blank")
    end
    cdf = CustomDataFlow.new
    cdf.owner = api_user_info.input_owner
    cdf.org = api_user_info.input_org
    cdf.status = Statuses[:init]
    cdf.update_mutable!(api_user_info, input)
    return cdf
  end

  def update_mutable! (api_user_info, input)
    return if (input.blank? || api_user_info.nil?)

    tags = input.delete(:tags)

    self.name = input[:name] if !input[:name].blank?
    self.description = input[:description] if input.key?(:description)
    self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
    self.org = api_user_info.input_org if (self.org != api_user_info.input_org)

    self.flow_type = CustomDataFlow.validate_flow_type_str(input[:flow_type]) if !input[:flow_type].blank?
    raise Api::V1::ApiError.new(:bad_request, "Invalid custom data flow type") if self.flow_type.nil?
    self.managed = !!input[:managed] if input.key?(:managed)
    self.config = input[:config] if input.key?(:config)

    # Note, allow direct setting of "status" by api caller.
    # The standard platform activate/pause mechanism is not
    # necessarily supported for off-platform flows.
    self.status = CustomDataFlow.validate_status_str(input[:status]) if input.key?(:status)
    raise Api::V1::ApiError.new(:bad_request, "Invalid status") if self.status.nil?

    ability = Ability.new(api_user_info.input_owner)
    self.update_associations(CodeContainer, ability, input)
    self.update_associations(DataCredentials, ability, input)

    self.save!

    ResourceTagging.add_owned_tags(self, {tags: tags}, api_user_info.input_owner)
  end

  def copy_post_save (original_flow, api_user_info, options)
    original_flow.code_containers.each do |cc|
      next if cc.nil?
      self.code_containers << (cc.reusable ? cc : cc.copy(api_user_info, options))
    end
    reuse_creds = !!options[:reuse_data_credentials]
    original_flow.data_credentials.each do |dc|
      next if dc.nil?
      self.data_credentials << (reuse_creds ? dc : dc.copy(api_user_info, options))
    end
  end

  def active?
    (self.status == Statuses[:active])
  end

  def activate!
    # Note, no ControlService messages here.
    if !self.active?
      self.status = Statuses[:active]
      self.save!
    end
  end

  def paused?
    (self.status == Statuses[:paused])
  end

  def pause!
    # Note, no ControlService messages here.
    if !self.paused?
      self.status = Statuses[:paused]
      self.save!
    end
  end

  def update_associations (model, ability, input, reset = false)
    model_under = model.name.underscore
    assoc_name = model_under.pluralize
    input_key = (model_under + "_ids").to_sym
    return if input[input_key].blank?

    input_ids = input[input_key]

    if (reset)
      self.send(assoc_name + "=", model.none)
      self.reload
    else
      current_ids = self.send(assoc_name).map(&:id)
      input_ids = input_ids.select { |aid| !current_ids.include?(aid) }.uniq
    end

    return if input_ids.empty?

    input_ids.each do |aid|
      as = model.find_by_id(aid)
      if (as.nil?)
        raise Api::V1::ApiError.new(:not_found, "Association not found: #{model_under}, #{cid}")
      end
      if (!ability.can?(:read, as))
        raise Api::V1::ApiError.new(:bad_request, "Invalid access to #{model_under.gsub("_", " ")}")
      end
      self.send(assoc_name) << as
    end
  end

  def reset_associations (model, ability, input)
    self.update_associations(model, ability, input, true)
  end

  def remove_associations (model, input)
    model_under = model.name.underscore
    assoc_name = model_under.pluralize
    input_key = (model_under + "_ids").to_sym
    attribute_sym = (model_under + "_id").to_sym
    return if input[input_key].blank?

    self.send(self.class.name.underscore.pluralize + "_" + assoc_name)
      .where(attribute_sym => input[input_key])
      .destroy_all
  end
end
