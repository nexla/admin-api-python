class DataSetValidator < ActiveModel::Validator
  def validate (data_set)
    # If the data_set is created within an org, the owner must be
    # a member of that org
    if (!data_set.org.nil? && !data_set.owner.org_member?(data_set.org))
      data_set.errors.add :base, "Owner not in data set organization: #{data_set.id}, #{data_set.owner_id}, #{data_set.org_id}"
    end

    if (!data_set.source_schema.blank? && data_set.parent_data_set.present?)
      data_set.errors.add :base, "Data set cannot have both a source schema and a parent data set"
    end
  end
end

class DataSet < ApplicationRecord
  self.primary_key = :id

  using Refinements::SchemaProperty

  include Api::V1::Schema
  include AuditLog
  include JsonAccessor
  include AccessControls::Sharing
  include Accessible
  include Summary
  include FlowNodeData
  include Copy
  include Chown
  include Docs
  include SearchableConcern
  include PaperTrailAssociations
  include UpdateRuntimeStatusConcern
  include UpdateAclNotificationConcern
  include DataplaneConcern
  include ReferencedResourcesConcern
  include ChangeTrackerConcern

  include RatingConcern
  rating :main

  ELASTICSEARCH_ATTRIBUTE_NAME = :schema_properties
  SPLITTER_OPERATION = 'nexla.splitter'.freeze
  UPSTREAM_SPLITTER_LOOKUP_LIMIT = 10
  DEFAULT_SPLITTER_RULES_LIMIT = 5

  validates :data_source_id,
    uniqueness: {
      scope: [:source_schema_id, :name],
      message: "%{value} already has an identical Nexset with this name — combination is not unique"
    },
    if: :requires_unique_source_schema_validation?

  validates_with DataSetValidator, if: :has_owner?

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :origin_node, class_name: "FlowNode", foreign_key: "origin_node_id"
  belongs_to :flow_node
  belongs_to :data_credentials
  belongs_to :code_container
  belongs_to :output_validator, class_name: "Validator", foreign_key: "output_validator_id"
  belongs_to :copied_from, class_name: "DataSet", foreign_key: "copied_from_id"

  attr_accessor :nexset_api_config
  attr_accessor :cascading_saves
  attr_accessor :control_messages_enabled
  attr_accessor :in_copy
  attr_accessor :skip_schema_detection
  attr_accessor :force_delete
  attr_accessor :parent_data_sink_id

  belongs_to :data_source, association_versions: true
  belongs_to :data_sample
  belongs_to :semantic_schema

  belongs_to :parent_data_set, class_name: "DataSet",
    foreign_key: "parent_data_set_id"
  has_many :child_data_sets, class_name: "DataSet", foreign_key: "parent_data_set_id"

  has_many :data_sinks
  has_many :external_sharers, dependent: :destroy
  has_many :data_maps, through: :data_sinks
  has_many :data_sets_api_keys, dependent: :destroy
  alias_method :api_keys, :data_sets_api_keys

  has_many :data_sets_catalog_refs, dependent: :destroy
  alias_method :catalog_refs, :data_sets_catalog_refs

  has_many :marketplace_items, dependent: :destroy
  has_many :domains, through: :marketplace_items
  has_one :endpoint_spec, dependent: :destroy

  acts_as_taggable_on :tags
  def tags_list
    self.tags.pluck(:name)
  end
  alias_method :tag_list, :tags_list

  before_create :build_flow_node
  after_create :handle_after_create
  after_save :handle_after_save
  after_commit :handle_after_commit_create, on: :create
  after_commit :handle_after_commit_update, on: :update
  before_update :handle_before_update
  after_update :handle_after_update
  after_destroy :handle_after_destroy
  after_save :handle_after_save
  after_initialize do
    self.nexset_api_config = nil
    self.cascading_saves = true
    self.control_messages_enabled = true
    self.in_copy = false
    self.skip_schema_detection = false
  end

  referencing_resources :data_maps, :data_credentials, :code_containers, :data_sets, :runtimes
  mark_as_referenced_resource

  json_accessor :source_path, :source_schema, :output_schema,
    :output_schema_annotations, :output_validation_schema,
    :custom_config, :runtime_config

  Samples_Size_Std_Dev = 170000
  Max_Cached_Samples = Rails.env.test? ? 5 : 20
  Min_Cached_Samples = 5

  Statuses = {
    :init     => 'INIT',
    :paused   => 'PAUSED',
    :active   => 'ACTIVE'
  }

  scope :with_api_keys, -> { where( Arel.sql("EXISTS(SELECT 1 FROM data_sets_api_keys WHERE data_sets_api_keys.data_set_id = data_sets.id)") )  }

  scope :for_search_index, -> { eager_load(:origin_node, data_source: [:vendor_endpoint, :connector] )}

  delegate :flow_type, to: :origin_node, allow_nil: true

  def self.backend_resource_name
    'dataset'.freeze
  end

  def self.shared_with_any_in_org (org)
    return DataSet.none unless org.present?

    ids = DataSetsAccessControl.where(
      role_index: DataSet.access_role_to_i(:sharer),
      accessor_org_id: org.id
    ).pluck(:data_set_id)

    DataSet.where(:id => ids)
  end

  def self.shared_with_user (user, org)
    return DataSet.none if user.nil?

    where_str = ""
    where_str += "accessor_org_id = #{org.id} and " if org.present?
    where_str += "(accessor_id = " + user.id.to_s + " and accessor_type = '" + AccessControls::Accessor_Types[:user] + "')"

    if !org.nil?
      where_str += " or (accessor_id = " + org.id.to_s + " and accessor_type = '" + AccessControls::Accessor_Types[:org] + "')"
    end

    teams = user.teams(org, access_role: :member)
    if !teams.empty?
      team_ids = teams.select(:id).map(&:id)
      where_str += " or (accessor_id in (" + team_ids.join(',') + ") and accessor_type = '" + AccessControls::Accessor_Types[:team] + "')"
    end

    ids = DataSetsAccessControl.where(
      role_index: AccessControls::ALL_ROLES_SET.index(:sharer)
    ).where(where_str).pluck(:data_set_id)

    DataSet.where(id: ids)
  end

  def self.derived_from_shared_or_public (user, org)
    return DataSet.none if (user.nil? && org.nil?)
    cnd = { :org => org }

    if (!user.nil?)
      pds = DataSet.shared_with_user(user, org).union(DataSet.where(:public => true)).select(:id)
      cnd[:owner] = user
    else
      pds = DataSet.shared_with_any_in_org(org).union(DataSet.where(:public => true)).select(:id)
    end

    DataSet.where(owner: user, org: org, parent_data_set_id: pds)
  end

  def self.all_condensed (filter_opts = {}, sort_opts = {})
    fields = [
      :id, :owner_id, :org_id,
      :status, :data_credentials_id, :runtime_status, :data_source_id,
      :parent_data_set_id, :updated_at, :created_at
    ]
    DataSet.where(filter_opts).select(fields).order(sort_opts)
  end

  def searchable_attributes
    source_schema = get_schema_properties(self.source_schema)
    output_schema = get_schema_properties(self.output_schema)
    synthetic = {
      source_schema_properties: source_schema,
      output_schema_properties: output_schema,
      attr_name: output_schema,
      schema_attr: output_schema,
      source_type: source_type,
      project_id: origin_node&.project_id,
      connector_code: connector_code,
      connector_name: connector_name,
      nexset_api_compatible: origin_node&.nexset_api_compatible,
      flow_type: origin_node&.flow_type,
      rag: (origin_node&.flow_type == FlowNode::Flow_Types[:rag])
    }

    data = self.attributes.merge(synthetic)
    data.delete('data_samples')
    data
  end

  def cascading_saves?
    self.cascading_saves = true if (!defined?(self.cascading_saves) || self.cascading_saves.nil?)
    self.cascading_saves
  end

  def disable_cascading_saves
    self.cascading_saves = false
  end

  def enable_cascading_saves
    self.cascading_saves = true
  end

  def self.generate_name_and_description (input, data_source)
    if !input[:sample_service_id].blank?
      schema_num = " ##{input[:sample_service_id]}"
    else
      schema_num = ""
    end
    name_str = data_source.name.blank? ? "Detected Data Set, " : "#{data_source.name}, " +
      "Schema#{schema_num}"
    desc_str = "Schema#{schema_num} detected from data source" +
      (data_source.name.blank? ? "" : ": #{data_source.name}")

    [name_str, desc_str]
  end

  def self.handle_duplicate_creation (data_set, msg)
    response = { :message => msg }
    existing_data_set = DataSet.where(data_source_id: data_set.data_source_id,
      source_schema_id: data_set.source_schema_id, name: data_set.name).first
    response[:data_set_id] = existing_data_set&.id
    raise Api::V1::ApiError.new(:conflict, response)
  end

  def self.build_from_input (api_user_info, input, use_source_owner = false, detected = false)
    return nil if (!input.is_a?(Hash) || api_user_info.nil?)

    input.symbolize_keys!
    ability = Ability.new(api_user_info.input_owner)
    data_source = nil

    DataSet.transaction do
      if input[:data_source_id].present?
        data_source = DataSource.find(input[:data_source_id])
        if (!ability.can?(:manage, data_source))
          raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data source")
        end
        if !data_source.flow_node.present?
          # FN backwards-compatibility. This can be removed after
          # flow-nodes conversion script completes.
          ActiveRecord::Base.logger.info("FLOW-NODES: converting data_source on-demand: #{data_source.id}")
          FlowNode.build_flow_from_data_source(data_source)
        end

        if data_source.flow_node.replication? && data_source.data_sets_ids.count == 1 # If replication flow has already 1 DataSet created
          data_set = data_source.data_sets.first
          data_set.update_mutable!(api_user_info, input, nil)
          return data_set
        end
      end

      if (!input[:source_schema].blank?)
        # source_schema_id must be set for validation in DataSet.create(), NEX-3613
        input[:source_schema_id] = input[:source_schema]["$schema-id"]
      end

      if input[:parent_data_sets].present?
        # Backwards-compatibility. We continue to accept parent_data_sets
        # array input, but only use the first id from it. In practice, no
        # callers ever passed more than one parent data_set id.
        input[:parent_data_set_id] = input[:parent_data_sets].first
        input.delete(:parent_data_sets)
      end

      if input[:parent_data_set_id].present?
        if (!data_source.nil?)
          raise Api::V1::ApiError.new(:bad_request, "Detected data set cannot also have a parent data set")
        end
        if (!input[:source_schema].blank?)
          raise Api::V1::ApiError.new(:bad_request, "Data set cannot have both a source schema and a parent data set")
        end
        parent_data_set = DataSet.find(input[:parent_data_set_id])
        if (!ability.can?(:read, parent_data_set))
          raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to parent data set")
        end
        if parent_data_set.flow_type == FlowNode::Flow_Types[:replication]
          raise Api::V1::ApiError.new(:bad_request, "Data set cannot be built from a parent Data set of replication type")
        end
      end

      if !data_source.nil?
        input[:status] = data_source.status
      end

      if (!api_user_info.user.super_user? && (input.key?(:public) || input.key?(:managed)))
        raise Api::V1::ApiError.new(:bad_request, "Input cannot include public or managed attribute")
      end

      if (input[:name].blank? && !data_source.nil? && detected)
        input[:name], input[:description] = self.generate_name_and_description(input, data_source)
      end

      if (use_source_owner)
        input[:owner_id] = data_source.owner.id
        input[:org_id] = (data_source.org.nil? ? nil : data_source.org.id)
      else
        input[:owner_id] = api_user_info.input_owner.id
        input[:org_id] = (api_user_info.input_org.nil? ? nil : api_user_info.input_org.id)
      end

      tags = input[:tags]
      input.delete(:tags)

      transform = input[:transform]
      input.delete(:transform)

      transform_id = (input[:transform_id] || input[:code_container_id])
      input.delete(:transform_id)
      input.delete(:code_container_id)
      has_custom_transform = false

      if (!transform_id.nil?)
        # An input transform_id overrides input transform code.
        # DataSet transform assignment (below) accepts either.
        transform = CodeContainer.find(transform_id)

        if transform.present? && transform.resource_type == CodeContainer::Resource_Types[:splitter] && transform.data_sets.count > 0
          raise Api::V1::ApiError.new(:bad_request, "Cannot attach a splitter transform that is already in use by another nexset")
        end

        if (!ability.can?(:read, transform))
          raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to transform")
        end
        # NEX-3485 When an existing transform_id is included in a POST, make
        # sure the transform is reusable AND add it to the input hash for the
        # for DataSet.create() call below.

        # NEX-15684: allow non-reusable transforms to be used as rerankers if they are free
        if !transform.reusable? && !transform.available_as_reranker?
          raise Api::V1::ApiError.new(:bad_request, "Input transform is not reusable")
        end

        has_custom_transform = (transform.code_type != CodeContainer::Code_Types[:jolt_standard])
        input[:code_container_id] = transform_id
      end

      output_schema_validation_enabled = input.delete(:output_schema_validation_enabled)
      output_validation_schema = input.delete(:output_validation_schema)
      output_validator_id = input.delete(:output_validator_id)

      has_custom_transform = has_custom_transform || !!input[:has_custom_transform]
      input.delete(:has_custom_transform)

      data_sinks = input.delete(:data_sinks)
      runtime_config = input.delete(:runtime_config)
      data_samples = input.delete(:data_samples)
      semantic_schema = input.delete(:semantic_schema)
      endpoint_spec = input.delete(:endpoint_spec)
      parent_data_sink_id = input.delete(:parent_data_sink_id)

      if input[:data_credentials_id].present?
        dc = DataCredentials.find(input[:data_credentials_id].to_i)
        if !ability.can?(:read, dc)
          raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data credentials")
        end
      end

      if data_source&.node_type == :api_server && endpoint_spec&.[]('method').present?
        default_mapping = data_source.endpoint_mappings&.exists?(method: endpoint_spec['method'], route: '/')
        count = data_source.endpoint_mappings&.where(method: endpoint_spec['method'])&.count
        input[:name] = input[:name] + " #{count}" if default_mapping
      end

      begin
        ref_fields = input.delete(:referenced_resource_ids)
        data_set = DataSet.new(input)
        data_set.set_defaults(api_user_info.input_owner, api_user_info.input_org) if parent_data_sink_id.present?

        # See NEX-12773. We allow POST caller to set output_schema
        # explicitly and bypass schema detection.
        data_set.skip_schema_detection = input[:output_schema].is_a?(Hash)

        data_set.verify_ref_resources!(api_user_info.user, ref_fields)
        data_set.save!
        data_set.update_referenced_resources(ref_fields)
        data_set.update_endpoint_spec(api_user_info, endpoint_spec) if endpoint_spec.present? && data_set.flow_type == FlowNode::Flow_Types[:api_server]
      rescue ActiveRecord::RecordNotUnique => e
        DataSet.handle_duplicate_creation(data_set, "(data_source_id, source_schema_id, name) is not unique!")
      rescue ActiveRecord::RecordInvalid => e
        if e.message =~ /is not unique/
          DataSet.handle_duplicate_creation(data_set, "(data_source_id, source_schema_id, name) is not unique!")
        else
          raise
        end
      end

      if !data_set.valid?
        msg = data_set.errors.full_messages.join(";")
        if (msg.include?("not unique"))
          DataSet.handle_duplicate_creation(data_set, msg)
        else
          raise Api::V1::ApiError.new(:bad_requests, msg)
        end
      end

      data_set.send(:update_schema_properties)
      data_set.send(:update_child_data_sets)

      # Use the input transform object, if one was included
      # AND no resusable transform id was passed. (NEX-3485)
      data_set.transform = transform if (!transform.blank? && transform_id.blank?)

      data_set.update_output_schema_validation!(
        api_user_info,
        output_schema_validation_enabled,
        output_validation_schema,
        output_validator_id
      )
      data_set.set_has_custom_transform!(has_custom_transform)
      data_set.runtime_config = runtime_config if runtime_config.present?

      data_set.data_samples = data_samples if data_samples.present?
      data_set.semantic_schema = semantic_schema if semantic_schema.present?
      data_set.save!

      ResourceTagging.add_owned_tags(data_set, { tags: tags }, api_user_info.input_owner)

      if (!data_sinks.blank? && data_sinks.is_a?(Array))
        data_set.update_data_sinks(api_user_info.input_owner, data_sinks)
      end

      data_set
    end
  end

  def get_nexset_api_config
    return nil if self.splitter? || self.upstream_has_splitter?

    if self.nexset_api_config.nil?
      if self.origin_node.present?
        ds = self.parent_source
      else
        # FN backwards-compatibility, check old-style origin
        ds = self.flow_origin
      end
      if ds.is_a?(DataSource)
        self.nexset_api_config = ds.nexset_api_config
        if self.nexset_api_config.is_a?(Hash)
          url = EnvironmentUrl.instance.nexset_api_url(self)
          self.nexset_api_config[:url] = url if url.present?
        end
      end
    end
    return self.nexset_api_config
  end

  def active_catalog_ref
    self.catalog_refs.with_active_catalog_config.first
  end

  def set_defaults (user, org)
    self.owner = user
    self.org = org
  end

  def update_mutable! (api_user_info, input, request)
    return if (input.blank? || api_user_info.nil?)

    ref_fields = input.delete(:referenced_resource_ids)
    verify_ref_resources!(api_user_info.input_owner, ref_fields)

    tags = input.delete(:tags)
    ability = Ability.new(api_user_info.input_owner)

    self.transaction do
      force_output_schema_update = false

      self.name = input[:name] if !input[:name].blank?
      self.description = input[:description] if input.key?(:description)
      self.source_path = input[:source_path] if (input.key?(:source_path))
      self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
      self.org = api_user_info.input_org if (self.org != api_user_info.input_org)

      # See NEX-12773 bypass schema detection if caller includes
      # desired output_schema in input...
      self.skip_schema_detection = input[:output_schema].is_a?(Hash)

      # samples update should go before transform update, because splitter transform may use samples to create child data sets
      if (input[:data_samples].is_a?(Array))
        self.data_samples = input[:data_samples][0...DataSet::Max_Cached_Samples]
      end

      input[:transform_id] = input.delete(:code_container_id) if input.key?(:code_container_id)
      if input[:transform_id].present?
        code_container = CodeContainer.find(input[:transform_id])
        if code_container.present? && code_container.resource_type == CodeContainer::Resource_Types[:splitter] && code_container.data_sets.count > 0
          raise Api::V1::ApiError.new(:bad_request, "Cannot attach a splitter transform that is already in use by another nexset")
        end
      end

      if (input.key?(:transform_id) || input.key?(:transform))
        if self.parent_data_set&.splitter?
          raise Api::V1::ApiError.new(:bad_request, "Cannot update transform for splitter branch data set")
        end

        force_output_schema_update = self.update_transform(request, api_user_info.input_owner, input)
      end

      if (input.key?(:output_validator_id))
        self.update_output_validator(api_user_info, input[:output_validator_id])
      end

      if input[:parent_data_sets].present?
        # Backwards-compatibility. We continue to accept parent_data_sets
        # array input, but only use the first id from it. In practice, no
        # callers ever passed more than one parent data_set id.
        input[:parent_data_set_id] = input[:parent_data_sets].first
      end

      if input[:parent_data_set_id].present?
        if self.parent_data_set&.splitter?
          raise Api::V1::ApiError.new(:bad_request, "Cannot update parent data set for splitter branch data set")
        end

        parent_data_set = DataSet.find(input[:parent_data_set_id])
        if !ability.can?(:read, parent_data_set)
          raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to parent data set")
        end
        self.parent_data_set = parent_data_set
      end

      # NOTE :data_source_id is ignored if it is included in
      # the input. No error is generated if parent_data_set_id
      # is also set, but no changes are made either. This is
      # legacy behavior.

      if (!input[:source_schema].blank?)
        self.source_schema = input[:source_schema]
        self.parent_data_set = nil
      end

      if (input.key?(:public) || input.key?(:managed))
        if (!api_user_info.user.super_user?)
          raise Api::V1::ApiError.new(:method_not_allowed, "Input cannot include public or managed attribute")
        end
        self.public = !!input[:public] if input.key?(:public)
        self.managed = !!input[:managed] if input.key?(:managed)
      end

      # Note, this will be overriden by "$schema-id"
      # in the source_schema, if it exists.
      self.source_schema_id = input[:source_schema_id] if input.key?(:source_schema_id)

      if (input.key?(:output_schema))
        self.output_schema = input[:output_schema]
      end

      if (input.key?(:output_schema_annotations))
        self.output_schema_annotations = input[:output_schema_annotations]
      end

      if (input.key?(:output_schema_validation_enabled))
        self.output_schema_validation_enabled = input[:output_schema_validation_enabled]
      end

      if (input.key?(:output_validation_schema))
        self.output_validation_schema = update_validation_schema_info(input[:output_validation_schema])
      end

      if (input.key?(:output_validator_id))
        self.output_validator_id = input[:output_validator_id]
      end

      if (input.key?(:custom_config))
        self.custom_config = input[:custom_config]
      end

      if (input.key?(:runtime_config))
        self.runtime_config = input[:runtime_config]
      end

      if input.key?(:data_credentials_id)
        dc = input[:data_credentials_id].nil? ? nil : DataCredentials.find(input[:data_credentials_id].to_i)
        if dc.present? && !ability.can?(:read, dc)
          raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data credentials")
        end
        self.data_credentials = dc
      end

      self.set_has_custom_transform!(input[:has_custom_transform]) if input.key?(:has_custom_transform)
      self.semantic_schema = input[:semantic_schema] if input.key?(:semantic_schema)

      # IMPROVE: refactor to avoid to double-save!
      # Check if we're saving changes that require a schema update below (new data model only);
      force_output_schema_update = (force_output_schema_update || self.update_output_schema_required?)
      self.save!

      if input.key?(:endpoint_spec)
        self.update_endpoint_spec(api_user_info, input[:endpoint_spec])
        force_output_schema_update = true if self.data_samples_changed?
      end
      self.update_schema_properties(force_output_schema_update)
      self.update_child_data_sets
      self.update_referenced_resources(ref_fields)

      self.save!

      if (input[:data_sinks].is_a?(Array))
        update_data_sinks(api_user_info.input_owner, input[:data_sinks])
      end

      ResourceTagging.add_owned_tags(self, {tags: tags}, api_user_info.input_owner)
    end
  end

  def build_api_key_from_input (api_user_info, input)
    api_key = DataSetsApiKey.new
    api_key.owner = self.owner
    api_key.org = self.org
    api_key.data_set_id = self.id
    api_key.update_mutable!(api_user_info, input)
    return api_key
  end

  def copy_pre_save (original_data_set, api_user_info, options = {})
    self.in_copy = true

    # Note, we don't copy the association with a source,
    # if one exists, because that would create a duplicate
    # combination of source_schema_id and data_source_id,
    # which would break schema updates from the infrastructure.
    self.data_source_id = nil

    # NEX-4763 don't copy data_samples. The metadata is
    # incorrect for copied samples (refers to original
    # set and it's processing history).
    # Note also: we must use write_attribute() to null out the
    # old embedded :data_samples attribute.
    self.write_attribute(:data_samples, nil)
    self.data_sample_id = nil

    # We don't copy the original's flow_node. The before_create
    # handler will create a new flow node for the copied data set.
    self.flow_node = nil
  end

  def from_shared? (parent_data_set = nil)
    parent_data_set ||= self.parent_data_set

    return false if self.data_source.present? || !parent_data_set.present?
    return false if self.in_copy
    return true if (self.org_id != parent_data_set.org_id)
    return false if (self.owner_id == parent_data_set.owner_id)

    # Here we check if the owner of the data set has collaborator
    # access to the parent. That includes :collaborator and :admin
    # access (:admin is a superset role). If not, the only access
    # to the parent data set remaining is :sharer.
    # Note: we must set the user's org context for this check.
    self.owner.org = parent_data_set.org
    return !parent_data_set.has_collaborator_access?(self.owner)
  end

  def empty_transform?
    self.transform["transforms"].empty?
  end

  def transform
    return Transform.empty_api_wrapper_transform if self.code_container.nil?
    return Transform.empty_api_wrapper_transform
      .merge({'custom_config' => self.code_container.custom_config}) if self.code_container.ai_function_type.present?
    Transform.find(self.code_container.id).get_jolt_spec(self)
  end

  def transform_optional
    Transform.find_by(id: self.code_container_id)&.get_jolt_spec(self)
  end

  def transform= (tx)
    if (tx.is_a?(CodeContainer) || tx.is_a?(Transform))
      if (!tx.is_output_record? && tx.ai_function_type.nil?)
        raise Api::V1::ApiError.new(:bad_request, "Invalid code container for transform")
      end
      self.transaction do
        prev_cc = self.code_container
        self.code_container_id = tx.id
        self.save!
        prev_cc&.maybe_destroy
      end
      return
    end

    # Put the incoming tx into Jolt spec format,
    # i.e. an array of Jolt operation specs...
    tx.symbolize_keys! if tx.is_a?(Hash)
    tx = nil if tx.blank?

    code_type = CodeContainer::Code_Types[:jolt_standard]
    custom_config = nil
    have_custom_config = false

    if (Transform.api_wrapper_format?(tx))
      code_type = CodeContainer::Code_Types[:jolt_custom] if !!tx[:custom]
      if tx.key?(:custom_config)
        have_custom_config = true
        custom_config = tx[:custom_config]
      end
      tx = tx[:transforms]
    else
      code_type = self.code_container.code_type if !self.code_container.nil?
    end

    tx = [tx] if !tx.nil? && !tx.is_a?(Array)

    self.transaction do
      delete_cc = nil

      if (tx.blank? && (code_type == CodeContainer::Code_Types[:jolt_standard]))
        delete_cc = self.code_container
        self.code_container = nil
      elsif (self.code_container.nil?)
        splitter_tx = update_splitter_children(tx)
        tx = splitter_tx if splitter_tx.present?

        input = {
          :owner => self.owner,
          :org => self.org,
          :resource_type => splitter_tx ? CodeContainer::Resource_Types[:splitter] : CodeContainer::Resource_Types[:transform],
          :reusable => false,
          :output_type => CodeContainer::Output_Types[:record],
          :code_type => code_type,
          :code => tx
        }
        input[:name] = (self.name || "Data set #{self.id}")
        input[:name] += splitter_tx ? " (Splitter)" : " (Transform)"
        if !self.description.blank?
          suffix = " (Transform)"
          limit = CodeContainer.columns_hash['description'].limit
          if self.description.size > limit - suffix.size
            size_limit = limit - 1 - suffix.size - 3
            input[:description] = self.description[0..size_limit] + '...' + suffix
          else
            input[:description] = self.description + suffix
          end
        end
        input[:custom_config] = custom_config if have_custom_config
        cc = CodeContainer.create(input)
        self.code_container = cc
      else
        splitter_tx = update_splitter_children(tx)
        tx = splitter_tx if splitter_tx.present?
        self.code_container.code = tx
        self.code_container.code_type = code_type
        self.code_container.custom_config = custom_config if have_custom_config
        self.code_container.save!
      end

      self.update_output_schema(true)
      self.save!

      delete_cc&.maybe_destroy
    end
  end

  def transform_changed?
    return true if self.code_container_id_changed?
    return false if self.code_container.nil?
    (self.code_container.code_changed? || self.code_container.code_config_changed?)
  end

  def update_data_sinks (user, input_sinks)
    ab = Ability.new(user)
    new_data_sinks = []

    input_sinks.each do |ds_id|
      ds = DataSink.where(:id => ds_id)[0]
      raise Api::V1::ApiError.new(:bad_request, "Required data sink not found") if ds.nil?
      raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data sink") if !ab.can?(:manage, ds)
      new_data_sinks << ds
    end

    self.data_sinks.each do |ds|
      if (input_sinks.include?(ds.id))
        input_sinks.delete(ds.id)
        next
      end
      # This data sink is not in the new list.
      # Detach it from this dataset.
      ds.data_set_id = nil
      ds.save!
    end

    new_data_sinks.each do |ds|
      ds.data_set_id = self.id
      ds.save!
    end

    self.data_sinks.reload
  end

  def update_endpoint_spec(api_user_info, input)
    return unless self.flow_type == FlowNode::Flow_Types[:api_server]
    return unless self.data_source.present? && self.data_source.node_type == :api_server
    return if input.blank?

    if self.endpoint_spec.nil?
      input[:data_set_id] = self.id
      default_mapping = self.data_source.endpoint_mappings.exists?(method: input['method'] || 'GET', route: '/')
      input[:path_params] = [{ key: "param_#{self.name.split(' ').last}" }] if default_mapping
      self.endpoint_spec = EndpointSpec.build_from_input(api_user_info, input)
    else
      self.endpoint_spec.update_mutable!(api_user_info, input)
    end

    self.source_schema = nil
    self.data_samples = [self.endpoint_spec.sample]
  end

  def is_source?
    !self.data_source_id.nil?
  end
  alias_method :is_detected?, :is_source?

  def is_isolated?
    (self.data_source_id.nil? && self.parent_data_set.nil?)
  end

  def data_sink_ids
    self.data_sinks.map(&:id)
  end

  def flow_attributes (user, org)
    [
      :data_source_id,
      :parent_data_set_id,
      :data_credentials_id,
      :code_container_id,
      :data_sink_ids,
      :public,
      :managed
    ].map { |attr| [attr, self.send(attr)] }
  end

  def flow_shared_attributes (user, org)
    [
      [ :public, self.public ]
    ]
  end

  def flow_origin (user = nil, org = nil)
    same_org = false

    if (user.nil?)
      flow_owner = self.owner
      flow_org = self.org
    else
      flow_owner = user
      flow_owner.org = flow_org = org
    end

    src = nil
    prev_ds = ds = self

    while (!ds.nil?) do
      if (ds.org != flow_org)
        ds = prev_ds
        break
      end

      if ((ds.owner != flow_owner) && (!ds.has_collaborator_access?(flow_owner)))
        ds = prev_ds
        break
      end

      if (ds.is_source?)
        src = DataSource.find_by_id(ds.data_source_id)
        break
      end

      prev_ds = ds
      ds = ds.parent_data_set
    end

    if (!src.nil? && src.has_collaborator_access?(flow_owner))
      return src
    end

    return ds
  end

  def get_latest_version
    return 1
    # DISABLED for now. The version attribute is not being used, and
    # this implementation generates a lot of queries to fetch it.
    # latest_version = DataSetVersion.where(:item_id => self.id).order(created_at: :desc).limit(1).select(:id).map { |d| d.id }
    # (!latest_version.nil? && !latest_version.empty?) ? latest_version.first : 1
  end

  def active?
    self.status == Statuses[:active]
  end

  def paused?
    self.status == Statuses[:paused]
  end

  def activate!
    return if self.active?
    self.transaction do
      if self.parent_data_set&.splitter?
        self.parent_data_set.child_data_sets.each do |child|
          child.status = Statuses[:active]
          child.flow_node.status = child.status
          child.save!
        end
      end

      self.status = Statuses[:active]
      self.flow_node.status = self.status
      self.flow_node.save!
      self.save!
      self.reload

      if self.splitter?
        self.child_data_sets.each do |child|
          child.status = Statuses[:active]
          child.flow_node.status = child.status
          child.save!
        end
      end
    end

    if self.active? # what is the reason to have this check?
      self.send_control_event(:activate)
      Notifications::ResourceNotifier.new(self, :activate).call
    end
  end

  def pause!
    return if self.paused?
    self.transaction do
      if self.parent_data_set&.splitter?
        self.parent_data_set.child_data_sets.each do |child|
          child.status = Statuses[:paused]
          child.flow_node.status = child.status
          child.save!
        end
      end

      self.status = Statuses[:paused]
      self.flow_node.status = self.status
      self.flow_node.save!
      self.save!
      self.reload

      if self.splitter?
        self.child_data_sets.each do |child|
          child.status = Statuses[:paused]
          child.flow_node.status = child.status
          child.save!
        end
      end
    end
    if self.paused? # what is the reason to have this check?
      self.send_control_event(:pause)
      Notifications::ResourceNotifier.new(self, :pause).call
    end
  end

  def output_schema_with_annotations
    return self.output_schema.schema_merge_annotations(self.output_schema_annotations)
  end

  def source_schema_with_annotations
    return self.source_schema.schema_merge_annotations(self.output_schema_annotations)
  end

  def has_custom_transform?
    return self.code_container.is_jolt_custom? if !self.code_container.nil?
    return false
  end

  def set_has_custom_transform! (is_custom = true)
    return if self.code_container.present?
    tx = self.transform
    if tx.blank?
      tx = {
        :version     => 1,
        :data_maps   => [],
        :transforms  => []
      }
    end
    tx[:custom] = !!is_custom
    self.transform = tx
    self.save!
  end

  def prepare_data_samples_with_metadata (samples)
    new_samples = Array.new
    return new_samples if (!samples.is_a?(Array) || samples.empty?)

    samples.each do |s|
      # Note, we filter out any samples that are not in Hash format.
      # Some older data sets have cached samples that are arrays.
      # See NEX-3751.
      next if !s.is_a?(Hash)

      raw = nil
      # Note, not using TransformService.new here because
      # no connection to infrastructure service is required
      # and the loading of configuration in TransformService.new
      # generates a lot of extra db requests when &include_samples=1
      # is passed to a GET /data_sets request that returns
      # many data sets (see NEX-10442, for example).
      meta = TransformService.generate_sample_metadata(self)

      if s.key?("rawMessage")
        raw = s["rawMessage"]
        meta = s["nexlaMetaData"] if s.key?("nexlaMetaData")
      else
        raw = s
      end

      new_samples << {
        "rawMessage" => raw,
        "nexlaMetaData" => meta
      }
    end

    return new_samples
  end

  def samples (options = {})
    count = options[:count].to_i
    count = -1 if (count <= 0)
    return self.get_live_samples(count, options) if options[:live].truthy?
    return options[:output_only] ?
      self.get_output_only_samples(count) :
      self.get_input_output_samples(count)
  end

  def get_output_only_samples (count)
    samples = self.data_samples
    
    if self.parent_data_set&.splitter?
      filtered_samples = get_splitter_child_samples(count) || []
      return prepare_data_samples_with_metadata(count > 0 ? filtered_samples[0...count] : filtered_samples) if filtered_samples.present?
    end
    
    if self.splitter?
      splitter_samples = get_splitter_transform_output(count) || []
      return prepare_data_samples_with_metadata(count > 0 ? splitter_samples[0...count] : splitter_samples) if splitter_samples.present?
    end
    
    if ((samples.size < count) || samples.empty?)
      if self.parent_data_set.present? && !self.parent_data_set.splitter?
        parent_samples = self.parent_data_set.get_output_only_samples(count)
        tx = self.apply_transform(parent_samples)
        if (tx[:status] == :ok || (tx[:status] == 200 && !tx[:output].blank?))
          samples = tx[:output]
        end
      end
    end

    samples = samples[0...count] if (count > 0)
    return prepare_data_samples_with_metadata(samples)
  end

  def get_input_output_samples (count)
    return self.get_output_only_samples(count) if (self.is_detected? || self.is_isolated?)

    if self.parent_data_set&.splitter?
      filtered_samples = get_splitter_child_samples(count) || []
      sliced_samples = count > 0 ? filtered_samples[0...count] : filtered_samples
      prepared_samples = prepare_data_samples_with_metadata(sliced_samples)
      
      return prepared_samples.map { |sample| { input: sample, output: sample } }
    end

    c_samples = self.data_samples
    p_samples = self.parent_data_set.data_samples

    need_more = ((c_samples.size < count) || c_samples.empty?)
    need_more = need_more || ((p_samples.size < count) || p_samples.empty?)
    need_more = need_more && (p_samples.size > c_samples.size)

    if need_more
      p_samples = self.parent_data_set.get_output_only_samples(count)
      tx = self.apply_transform(p_samples)
      c_samples = tx[:output]
    end

    p_samples = self.parent_data_set.prepare_data_samples_with_metadata(p_samples)
    c_samples = self.prepare_data_samples_with_metadata(c_samples)
    count = p_samples.size if (count <= 0)
    samples = Array.new

    p_samples.each_with_index do |input, idx|
      samples << { :input => input, :output => c_samples[idx] }
      break if (samples.size >= count)
    end

    return samples
  end

  def get_live_samples (count, options)
    live_samples = SampleService.new.samples(self, {
      :output_only => true,
      :count => count,
      from: options[:from]
    })

    include_metadata = (!options.key?(:include_metadata) || options[:include_metadata])

    if (live_samples[:status] == :ok)
      samples = live_samples[:output].is_a?(Array) ? live_samples[:output] : []
      samples = samples.pluck("rawMessage") if !include_metadata
      return samples
    else
      raise Api::V1::ApiError.new(live_samples[:status], "Sample service error")
    end
  end

  # TODO: remove on DataFlow code cleanup ?
  def self.is_valid_tree? (node, dsf_stack = [])
    return false if dsf_stack.detect { |id| id == node[:id] }
    dsf_stack << node[:id]

    node[:children].each do |child_node|
      return false if !DataSet.is_valid_tree?(child_node, dsf_stack)
    end

    dsf_stack.pop
    return true
  end

  def self.replace_node_parent (node, set_node, parent_id)
    if (node[:id] == set_node[:id])
      node[:parent_data_set_id] = parent_id
    end

    node[:children].each do |child_nodes|
      DataSet.replace_node_parent(child_nodes, set_node, parent_id)
    end

    if (node[:id] == parent_id)
      node[:children] << set_node
    end
  end

  def self.get_node_by_id (nodes, id)
    return nodes if nodes[:id] == id
    node = nil
    return node if nodes[:children].nil?
    nodes[:children].each do |cn|
      node = DataSet.get_node_by_id(cn, id)
      break if !node.nil?
    end
    return node
  end

  # TODO: remove on DataFlow code cleanup ?
  def is_valid_parent? (data_set, user)
    flows = self.flows(false, user)[:flows]
    self_node = DataSet.get_node_by_id(flows, self.id)
    DataSet.replace_node_parent(flows, self_node.deep_dup, data_set.id)
    return DataSet.is_valid_tree?(flows)
  end

  def child_flows (resources, origin_user, origin_org, user = nil, org = nil, admin_level = :none)
    tree = self.flow_node_data(resources)
    children = self.child_data_sets

    tree[:children] = []
    add_dependent_data_sources(resources, user, org)

    return tree if children.blank?

    children.each do |child|
      next if (child.org != origin_org) && (admin_level != :super)
      # NEX-5441 The following line was preventing accounts with
      # admin access to a flow from seeing child data sets they
      # added to it. Revisit this in flows rewrite.
      # next if (child.owner != origin_user) && (admin_level == :none)
      tree[:children] << child.child_flows(resources, origin_user, origin_org, user, org, admin_level)
    end

    return tree
  end

  def flows (downstream_only = false, user = nil, org = nil, admin_level = :none)
    origin_user = self.owner
    origin_user.org = origin_org = self.org

    resources = DataFlow.empty_flows
    nodes = self.child_flows(resources, origin_user, origin_org, user, org, admin_level)

    if (!downstream_only)
      ds = self.parent_data_set
      self.add_origin_data_sinks(resources) if ds.nil?

      while (!ds.nil?)
        is_shared = false
        is_collaborator = false

        if ((ds.org != origin_org) && (admin_level != :super))
          is_shared = ds.has_sharer_access?(origin_user, origin_org)
          break if !is_shared
        elsif ((ds.owner != origin_user) && (admin_level == :none))
          is_shared = ds.has_sharer_access?(origin_user, origin_org)
          is_collaborator = ds.has_collaborator_access?(origin_user)
          break if !is_shared && !is_collaborator
        end

        node = is_shared ? ds.flow_shared_node_data(resources, user, org) :
          ds.flow_node_data(resources, user, org)

        node[:children] = [nodes]
        nodes = node

        break if is_shared

        prev_ds = ds
        ds = ds.parent_data_set
        prev_ds.add_origin_data_sinks(resources) if ds.nil?
      end
    end

    resources[:flows] = nodes
    return resources
  end

  def add_dependent_data_sources (resources, user, org)
    self.data_sinks.each do |data_sink|
      next if (data_sink.sink_config.nil? || data_sink.sink_config[DataSink::Ingest_Data_Source_Key].blank?)
      data_source = DataSource.find_by_id(data_sink.sink_config[DataSink::Ingest_Data_Source_Key])
      next if data_source.nil?
      resources[:dependent_data_sources] <<
        data_source.flow_dependent_data_source_data(user, org)
    end
  end

  def add_origin_data_sinks (resources)
    return if (self.data_source.nil? || self.data_source.data_sink.nil?)
    resources[:origin_data_sinks] << {
      :id => self.data_source.data_sink.id,
      :owner_id => self.data_source.data_sink.owner_id,
      :org_id => self.data_source.data_sink.org_id
    }
  end

  def update_sharers (sharers, mode, current_org)
    mode = mode&.to_sym
    raise ArgumentError.new("Invalid sharer update mode") if ![:add, :remove, :reset].include?(mode)
    sharers = [sharers] if sharers.class != Array
    sharers2 = []


    DataSet.transaction do
      old_sharers = pluck_accessors(:sharer)

      if (mode == :reset)
        self.external_sharers.each(&:destroy)
      end

      sharers.each do |sharer|
        # Pre-flight the sharers. At least one of :user, :team or :org is required.
        # If :user is supplied, an org specifier is optional. If :team or :org is
        # supplied, the org is determined by the resource itself.
        sharer = sharer.symbolize_keys

        sharer[:user] = User.find_by_email(sharer[:email])
        sharer[:org_id] = current_org.id unless sharer.key?(:org_id)
        if sharer[:org_id].present?
          org_id = sharer[:org_id]
          org_id = sharer[:user].default_org.id if (org_id == "*") && sharer[:user].present?
          sharer[:org] = Org.find_by_id(org_id)
        end
        sharer[:team] = Team.find_by_id(sharer[:team_id])

        if (sharer[:user].nil? && !sharer[:email].blank?)
          if (mode == :add || mode == :reset)
            es = ExternalSharer.create(
              :data_set_id => self.id,
              :email => sharer[:email],
              :org_id => sharer[:org_id],
              :team_id => sharer[:team_id],
              :name => sharer[:name],
              :description => sharer[:description]
            )
            UserMailer.send_external_dataset_share_notification(self, sharer[:email]).deliver
          elsif (mode == :remove)
            ExternalSharer.where(:data_set_id => self.id, :email => sharer[:email]).destroy_all
          end
          # Filter out unknown sharers for now...
          next
        end

        if (sharer[:user].nil? && sharer[:org].nil? && sharer[:team].nil?)
          raise Api::V1::ApiError.new(:bad_request, "No user, team or org in sharer entry")
        end

        sharers2 << sharer
      end

      self.remove_all_sharers if (mode == :reset)

      sharers2.each do |sharer|
        case mode
        when :add, :reset
          if (!sharer[:user].nil?)
            self.add_sharer(sharer[:user], sharer[:org])
          elsif (!sharer[:team].nil?)
            self.add_sharer(sharer[:team], sharer[:org])
          else
            self.add_sharer(sharer[:org])
          end
        when :remove
          if (!sharer[:user].nil?)
            self.remove_sharer(sharer[:user], sharer[:org])
          elsif (!sharer[:team].nil?)
            self.remove_sharer(sharer[:team], sharer[:org])
          else
            self.remove_sharer(sharer[:org])
          end
        end
      end

      new_sharers = pluck_accessors(:sharer)

      added = new_sharers - old_sharers
      removed = old_sharers - new_sharers

      self.notify_acl_changed(id: self.id,
                              type: self.class.name,
                              added: format_accessors_for_notification(added),
                              removed: format_accessors_for_notification(removed))

      DataSetNotifySharersWorker.perform_async(self.id)
    end
  end

  def sharer_counts
    counts = { user: 0, team: 0, org: 0, external: 0, total: 0 }
    sharer_role_index = AccessControls::ALL_ROLES_SET.index(:sharer)
    self.access_controls.each do |ac|
      next if (ac.role_index != sharer_role_index)
      counts[:total] += 1
      counts[:external] += 1 if (ac.accessor_org_id != self.org&.id)
      case ac.accessor_type
      when AccessControls::Accessor_Types[:user]
        counts[:user] += 1
      when AccessControls::Accessor_Types[:team]
        counts[:team] += 1
      when AccessControls::Accessor_Types[:org]
        counts[:org] += 1
      end
    end
    counts
  end

  def sharers(reload: true)
    sharers = DataSetsAccessControl.where(:data_set_id => self.id).map(&:render)

    self.reload if reload

    {
      sharers: sharers.compact,
      external_sharers: self.external_sharers.map(&:render)
    }
  end

  def remove_all_sharers
    # Note: you cannot remove default sharers (e.g. owner,
    # admins and Org admins), only specific sharer grantees.
    self.access_controls.each do |ac|
      if ac.role_index == AccessControls::ALL_ROLES_SET.index(:sharer)
        ac.destroy
      end
    end
  end

  def force_save!
    self.update_schema_properties(true)
    self.save!
  end

  def apply_transform (data, accumulate_schema = false)
    return { :output => [], :status => :bad_request } if data.nil?

    tx = self.transform.symbolize_keys

    if (tx[:transforms].empty? && !accumulate_schema)
      result = { :output => data, :schemas => [], :status => :ok }
    else
      result = TransformService.new.transform(tx, data, {
        :accumulate_schema => accumulate_schema, :org => self.org })
    end

    return result
  end

  def can_destroy?
    ids = self.child_data_sets.map(&:id)
    if !ids.empty? && !self.splitter?
      return false, "Data set has one or more child data sets: #{ids.inspect}"
    end
    ids = self.data_sinks.select(&:active?).map(&:id)
    if !ids.empty?
      return false, "Data set has one or more active data sinks: #{ids.inspect}"
    end
    if self.active?
      return false, "Data set must be paused before deletion"
    end

    link = FlowLink.where(retriever_data_set_id: self.id).first
    if link.present?
      return false, "Data set is used as a retriever in a flow link (link ID #{link.id}, flow ID #{link.left_origin_node_id}). " \
    end

    if self.parent_data_set&.splitter? && !force_delete
      return false, "Cannot delete a splitter's child data set directly. Please modify splitter rules instead."
    end

    return true
  end

  def destroy
    # If already destroyed, return. Called by FlowNode.destroy as association.
    return self if DataSet.find_by(id: self.id).nil?

    ok, msg = self.can_destroy?
    raise Api::V1::ApiError.new(:method_not_allowed, msg) if !ok

    # FN backwards-compatibility
    # We no longer have an ActiveRecord association to parent data set
    # through data_sets_parent_data_sets, but we might have entries
    # in that table (during transition to full flow-nodes mode).
    # Delete any entries remaining explicitly. Similar applies to
    # projects_data_flows table.
    DataSet.transaction do
      DataSetsParentDataSet.where(data_set_id: self.id).destroy_all
      ProjectsDataFlow.where(data_set_id: self.id).destroy_all

      super
      flow_node&.destroy
      if self.splitter?
        self.child_data_sets.map(&:destroy)
      end

      Notifications::ResourceNotifier.new(self, :delete).call
    end
  end

  def update_output_schema_validation! (api_user_info,
    output_schema_validation_enabled,
    output_validation_schema,
    output_validator_id)

    self.output_schema_validation_enabled = output_schema_validation_enabled if !output_schema_validation_enabled.nil?
    if !output_validation_schema.blank?
      self.output_validation_schema = update_validation_schema_info(output_validation_schema)
    end

    self.update_output_validator(api_user_info, output_validator_id)

    if (
      output_schema_validation_enabled_changed? ||
      output_validation_schema_changed? ||
      output_validator_id_changed?
    )
      self.save!
    end
  end

  def update_validation_schema_info(output_validation_schema)
    if !output_validation_schema.blank?
      output_validation_schema["$schema-id"] = self.output_schema["$schema-id"]
      output_validation_schema = DataSet.add_validation_schema_info(output_validation_schema)
    end

    return output_validation_schema
  end

  def self.add_validation_schema_info(output_validation_schema)
    if !output_validation_schema.blank?
      output_validation_schema["$schema"] = "http://json-schema.org/draft-07/schema#"
      output_validation_schema["type"] = "object"
    end
    return output_validation_schema
  end

  def source_type
    data_source&.source_type
  end

  def semantic_schema=(schema)
    raise Api::V1::ApiError.new(:internal_server_error,
      "Unsupported class in semantic_schema assignment!") if (schema.present? && !schema.is_a?(Hash))

    # Silently ignore attempt to set :semantic_schema in
    # DataSet.create and DataSet.new...
    return if self.id.nil?

    SemanticSchema.transaction do
      if schema.nil?
        self.semantic_schema_id = nil
      else
        self.semantic_schema_id = SemanticSchema.create_for_data_set(self, schema).id
      end
    end
  end

  def semantic_schemas
    SemanticSchema.where(data_set_id: self.id).order(id: :desc)
  end

  def data_samples
    return self.data_sample.samples if self.data_sample.present?
    samples_json = read_attribute(:data_samples)
    return Array.new if samples_json.blank?
    begin
      samples = JSON.parse(samples_json)
    rescue JSON::ParserError => e
      samples = Array.new
    end
    samples
  end

  def data_samples=(samples)
    raise Api::V1::ApiError.new(:internal_server_error,
      "Unsupported class in data_samples assignment!") if !samples.is_a?(Array)

    # Silently ignore attempt to include :data_samples in
    # DataSet.create and DataSet.new...
    return if self.id.nil?

    DataSample.transaction do
      data_sample = DataSample.create_for_data_set(self, samples)
      self.data_sample_id = data_sample.id
      self.write_attribute(:data_samples, nil)

      self.update_splitter_data_samples if self.splitter?
    end
  end

  def process_audit_log_entries (entries)
    # This method is a callback from audit_log(), which is
    # implemented in the AuditLog module. If the callback
    # method is present, as it is here, audit_log() calls it
    # with the fetched array of versions table entries and
    # returns the processed array to its caller. This allows
    # for decorating the entries with synthetic info not
    # stored natively in the raw records.
    return entries.map do |e|
      changes = e.object_changes["data_sample_id"]
      if changes.is_a?(Array)
        # Note, in the case of changes == [nil, N], we might
        # have had embedded data_samples in the previous version.
        from_samples = changes.first.nil? ? e.object_changes["data_samples"]&.first :
          DataSample.find_by_id(changes.first.to_i)&.samples
        e.object_changes["data_samples"] = [
          from_samples,
          DataSample.find_by_id(changes.second.to_i)&.samples
        ]
      end

      changes = e.object_changes["semantic_schema_id"]
      if changes.is_a?(Array)
        # Cases: [nil, N], [N, M], [M, nil]
        e.object_changes["semantic_schema"] = [
          changes[0].nil? ? nil : SemanticSchema.find_by_id(changes[0].to_i)&.schema,
          changes[1].nil? ? nil : SemanticSchema.find_by_id(changes[1].to_i)&.schema
        ]
      end

      e
    end
  end

  def node_type
    return :splitter if self.code_container&.resource_type == CodeContainer::Resource_Types[:splitter]

    case self.flow_type
    when FlowNode::Flow_Types[:rag]
      if self.code_container_id.present?
        self.code_container&.resource_type == CodeContainer::Resource_Types[:ai_function] ? self.code_container.ai_function_type : :data_set
      else
        self.flow_type == FlowNode::Flow_Types[:rag] ? :data_retriever_holder : :data_set
      end
    when FlowNode::Flow_Types[:api_server]
      if self.endpoint_spec.present?
        :api_request
      elsif self.code_container_id.present?
        :api_success_modifier
      else
        self.runtime_config['node_type'] || :api_result
      end
    else
      :data_set
    end
  end

  def api_compatible?
    self.get_nexset_api_config.present?
  end

  def parent_source
    return self.data_source if self.data_source_id.present?
    return self.origin_node.data_source if self.origin_node&.data_source_id.present?

    self.parent_data_set&.parent_source
  end

  def upstream_has_splitter?
    return false if self.parent_data_set.nil?

    parent = self.parent_data_set
    depth = 0
    while parent && depth < UPSTREAM_SPLITTER_LOOKUP_LIMIT
      return true if parent.splitter?
      parent = parent.parent_data_set
      depth += 1
    end
    false
  end

  def connector
    self.parent_source&.connector
  end

  def update_splitter_children(transform)
    return nil if transform.blank?

    if transform.is_a?(Array) && transform.first.is_a?(Hash)
      tx = transform.first
    elsif transform.is_a?(Hash) && transform['operation'].present?
      tx = transform
    elsif transform.is_a?(Hash) && transform[:transforms].is_a?(Array) && transform[:transforms].first.is_a?(Hash)
      tx = transform[:transforms].first
    end

    return nil if tx['operation'] != SPLITTER_OPERATION

    if self.code_container.present? && self.code_container.resource_type != CodeContainer::Resource_Types[:splitter]
      raise Api::V1::ApiError.new(:bad_request, "Cannot assign splitter transform for a non-splitter data set")
    end

    rules = tx.dig('spec', 'rules') || []
    fallback_rule = tx.dig('spec', 'fallback_destination')

    if rules.present? && !rules.is_a?(Array)
      raise Api::V1::ApiError.new(:bad_request, "Invalid splitter rules")
    end

    if fallback_rule.present? && !fallback_rule.is_a?(Hash)
      raise Api::V1::ApiError.new(:bad_request, "Invalid fallback destination")
    end

    if rules.blank?
      raise Api::V1::ApiError.new(:bad_request, "Splitter must have at least one rule")
    end

    if rules.size > DataSet.splitter_rules_limit
      raise Api::V1::ApiError.new(:bad_request, "Splitter cannot have more than #{DataSet.splitter_rules_limit} rules")
    end

    rule_nexset_ids = rules.map{|r| r['destination']['id']}.compact
    rule_nexset_ids << fallback_rule['id'] if fallback_rule && fallback_rule['id'].present?

    to_delete_ids = children_ids_from_splitter - rule_nexset_ids
    to_delete = DataSet.where(id: to_delete_ids)
    to_delete.each do |ds|
      ds.force_delete = true
      ds.destroy!
    end

    # Hack for checking permissions error.
    # Must set the owner object's org context to use it for access-control checks.
    self.owner.org = self.org if self.owner && self.org
    api_user_info = ApiUserInfo.new(self.owner, self.org)

    rule_number = 0
    rules.each do |rule|
      if rule['destination']['type'] == 'nexset'
        if rule['destination']['id'].blank?
          name = rule['destination'].delete('name')
          data_set = DataSet.build_from_input(api_user_info, { name: name, source_schema: self.source_schema, parent_data_set_id: self.id })
          rule['destination']['id'] = data_set.id
        elsif !rule['destination']['id'].in?(rule_nexset_ids)
          raise Api::V1::ApiError.new(:bad_request, "Cannot change splitter rule destination to a different data set.")
        else
          if rule['destination']['name'].present?
            ds = DataSet.find_by(id: rule['destination']['id'])
            if ds && ds.name != rule['destination']['name']
              ds.name = rule['destination']['name']
              ds.save!
            end
          end
        end
        rule_number += 1
      end
    end

    existing_fallback = splitter_fallback_destination

    # We require fallback to be present. If not specified, default is 'discard'.
    if fallback_rule.nil?
      fallback_rule = {'type' => 'discard' }
      tx['spec']['fallback_destination'] = fallback_rule
    end

    if fallback_rule['type'] == 'discard'
      if existing_fallback.present?
        ds = DataSet.find_by(id: existing_fallback['id'])
        if ds
          ds.force_delete = true
          ds.destroy
        end
      end
    elsif fallback_rule['type'] == 'nexset'
      if  existing_fallback && existing_fallback['id'] != fallback_rule['id']
        raise Api::V1::ApiError.new(:bad_request, "Cannot change fallback destination to a different data set.")
      end

      if (existing_fallback && fallback_rule['id'].blank?) || (existing_fallback && fallback_rule.blank?)
        ds = DataSet.find_by(id: existing_fallback['id'])
        if ds
          ds.force_delete = true
          ds.destroy
        end
      end

      if fallback_rule['id'].blank?
        name = fallback_rule.delete('name')
        data_set = DataSet.build_from_input(api_user_info, {name: name, source_schema: self.source_schema, parent_data_set_id: self.id })
        fallback_rule['id'] = data_set.id
      end
    end

    transform
  end

  def reassign_splitter_children!
    return unless self.splitter?

    tx = self.transform['transforms']

    return unless tx.is_a?(Array) && tx.first.is_a?(Hash) && tx.first["operation"] == DataSet::SPLITTER_OPERATION

    splitter_tx = tx.first
    spec = splitter_tx["spec"]

    data_sets = DataSet.where(parent_data_set_id: self.id).order(id: 'asc')
    ds_index = 0

    spec['rules'].each do |rule|
      if rule.dig('destination', 'type') == 'nexset'
        ds = data_sets[ds_index]

        if ds
          rule['destination']['id'] = ds.id
        end

        ds_index += 1
      end
    end

    fallback = spec['fallback_destination']
    if fallback.present? && fallback['type'] == 'nexset'
      ds = data_sets[ds_index]
      if ds
        fallback['id'] = ds.id
      end
    end

    Transform.find(self.code_container.id).update(code: tx)
  end

  def delete_splitter
    if self.code_container && self.code_container.resource_type == CodeContainer::Resource_Types[:splitter]
      self.child_data_sets.each do |ds|
        ds.force_delete = true
        ds.destroy!
      end
      self.child_data_sets.reload
    end
  end

  def splitter?
    code_container&.resource_type == CodeContainer::Resource_Types[:splitter]
  end

  def update_splitter_data_samples
    if self.splitter?
      self.child_data_sets.reload.each do |ds|
        samples = get_splitter_data_samples(self.data_samples, ds.id)
        if samples.present? && ds.data_samples != samples
          ds.data_samples = samples
          ds.save!
        end
      end
    end
  end

  def self.splitter_rules_limit
    ENV['SPLITTER_RULES_LIMIT']&.to_i || DEFAULT_SPLITTER_RULES_LIMIT
  end

  def self.bulk_update_samples(origin_node_id, api_user_info, input, replace_samples = false)
    result = {
      data_set_ids: [],
      errors: []
    }

    input_hash = input['data_sets'].index_by { |ds| ds['id'] }
    return result if input_hash.blank?

    ids = input['data_sets'].pluck('id') || []
    data_sets = if api_user_info.user.super_user?
      DataSet.where(id: ids, origin_node_id: origin_node_id)
    else
      DataSet.accessible_by_user(api_user_info.user, api_user_info.org, { access_role: :all, selected_ids: ids })
        .where(origin_node_id: origin_node_id)
    end

    data_sets.find_each do |data_set|
      samples = input_hash[data_set.id]['data_samples']
      data_samples = samples.is_a?(Array) ? samples : [samples]
      data_samples += (replace_samples ? [] : data_set.data_samples)
      data_set.data_samples = data_samples[0...Max_Cached_Samples]

      output_schema = input_hash[data_set.id]['output_schema']
      if output_schema.present?
        input = {
          output_schema: output_schema
        }
        input.merge!({output_validation_schema: input_hash[data_set.id]['output_validation_schema']}) if input_hash[data_set.id]['output_validation_schema'].present?
        data_set.update_mutable!(api_user_info, input, nil)
      end

      data_set.save

      result[:data_set_ids] << data_set.id
    end
    result[:errors] = ids - result[:data_set_ids]

    result
  end

  protected

  def get_splitter_data_samples(samples, nexset_id)
    return [] unless samples.is_a?(Array)
    return [] unless samples[0].is_a?(Hash)
    return [] if nexset_id.blank?

    samples.select do |sample|
      sample['forkToDataset'].to_s == nexset_id.to_s
    end
  end

  def children_ids_from_splitter
    return [] if self.transform.blank? || self.transform['transforms'].blank?

    tx = self.transform['transforms'][0]
    return [] if tx['operation'] != SPLITTER_OPERATION

    rules = tx.dig('spec', 'rules')
    fallback_rule = tx['fallback_destination']

    ids = []
    rules.each do |rule|
      if rule['destination']['type'] == 'nexset' && rule['destination']['id'].present?
        ids << rule['destination']['id']
      end
    end

    if fallback_rule.present? && fallback_rule['type'] == 'nexset' && fallback_rule['id'].present?
      ids << fallback_rule['id']
    end

    ids
  end

  def disable_control_messages
    self.control_messages_enabled = false
  end

  def send_control_event (event_type)
    return if flow_type == FlowNode::Flow_Types[:rag]

    ControlService.new(self).publish(event_type) if self.control_messages_enabled
  end

  def requires_unique_source_schema_validation?
    !self.data_source_id.nil? && !self.source_schema_id.blank?
  end

  def has_owner?
    !self.owner.nil?
  end

  def handle_after_create
    self.flow_node.data_set_id = self.id
    self.flow_node.save!
  end

  def handle_after_commit_create
    self.send_control_event(:create)
  end

  def handle_after_commit_update
    self.send_control_event(:update)
  end

  def handle_before_update
    # Note, we want changes to flow_node, if any, to be made
    # inside any wrapping transaction so that they are also
    # rolled back in case of exception. That's why we use
    # before_update and after_update instead of after_commit.
    if (self.will_save_change_to_data_source_id? || self.will_save_change_to_parent_data_set_id?)
      parent = (self.data_source || self.parent_data_set)
      if parent.present?
        if !parent.flow_node.present?
          raise Api::V1::ApiError.new(:internal_server_error,
            "Missing parent flow node for data set #{self.id}: #{parent.class.name}, #{parent.id}")
        end
        parent_node = parent.flow_node
      else
        parent_node = nil
      end
      FlowNode.reset_flow_origin(self, parent_node)
    end
  end

  def handle_after_update
    # Note, we want changes to flow_node, if any, to be made
    # inside any wrapping transaction so that they are also
    # rolled back in case of exception. That's why we use
    # before_update and after_update instead of after_commit.
    if (self.saved_change_to_owner_id? || self.saved_change_to_org_id?)
      # Saving resets owner_id and org_id
      # from the associated data_set.
      self.api_keys.each(&:save!)
      self.flow_node.owner = self.owner
      self.flow_node.org = self.org
      self.flow_node.save!
    end
  end

  def handle_after_save
    return if !self.saved_changes.keys.include?("output_schema")

    begin
      DataSetAuditsWorker.perform_async(self.id)
    rescue => e
      logger = Rails.configuration.x.error_logger
      logger.error({
        class: "DataSetAuditsWorker",
        id: self.id,
        error: e.message
      }.to_json)
    end
  end

  def handle_after_destroy
    self.data_sinks.destroy_all
    if (!self.code_container_id.nil?)
      CodeContainer.find(self.code_container_id).maybe_destroy
    end
    self.send_control_event(:delete)
  end

  def build_flow_node (project = nil)
    return if self.flow_node.present?
    pn_id = nil
    on_id = nil
    son_id = nil

    if self.data_source.present?
      if !self.data_source.flow_node.present?
        raise Api::V1::ApiError.new(:internal_server_error,
          "Missing flow node for parent data source: #{self.data_source.id}")
      end
      pn_id = self.data_source.flow_node.id
      on_id = self.data_source.flow_node.origin_node_id
    elsif self.parent_data_set.present?
      if !self.parent_data_set.flow_node.present?
        raise Api::V1::ApiError.new(:internal_server_error,
          "Missing flow node for parent data set: #{self.parent_data_set.id}")
      end
      pn_id = self.parent_data_set.flow_node.id
      if self.from_shared?
        son_id = self.parent_data_set.flow_node.origin_node_id
      else
        on_id = self.parent_data_set.flow_node.origin_node_id
      end
    elsif self.parent_data_sink_id.present?
      ability = Ability.new(self.owner)
      parent_data_sink = DataSink.find(self.parent_data_sink_id)
      raise Api::V1::ApiError.new(:bad_request, "Parent data sink only allowed for API server flows") if parent_data_sink.node_type != :api_target
      raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to parent data set") unless ability.can?(:read, parent_data_sink)

      pn_id = parent_data_sink.flow_node.id
      on_id = parent_data_sink.flow_node.origin_node_id
    end

    flow_node = FlowNode.new({
      owner_id: self.owner_id,
      org_id: self.org_id,
      project: project,
      data_set_id: self.id,
      parent_node_id: pn_id,
      origin_node_id: on_id,
      shared_origin_node_id: son_id,
      name: self.flow_name.blank? ?
        self.name : self.flow_name,
      description: self.flow_description.blank? ?
        self.description : self.flow_description,
      status: self.status,
      managed: self.managed
    })

    flow_node.save!
    if on_id.nil?
      # Can we avoid this double-save on origin flow_nodes?
      flow_node.origin_node_id = flow_node.id
      flow_node.save!
    end

    self.flow_node_id = flow_node.id
    self.origin_node_id = flow_node.origin_node_id
  end

  def update_schema_properties(force = false)
    update_output_schema(force)
    update_schema_ids
  end

  def update_child_data_sets
    if self.cascading_saves?
      self.child_data_sets.each(&:force_save!)
    end
  end

  def update_schema_ids
    if !self.source_schema.blank? && self.source_schema.key?("$schema-id")
      self.source_schema_id = self.source_schema["$schema-id"]
    end
  end

  def update_output_schema_required?
    self.transform = {
      :version     => 1,
      :data_maps   => [],
      :transforms  => []
    } if self.transform.blank?

    return false if !self.cascading_saves?

    return (
      self.source_schema_changed? ||
      self.parent_data_set_id_changed? ||
      self.transform_changed? ||
      self.output_schema.blank?
    )
  end

  def update_output_schema (force = false)
    return if !force && !self.update_output_schema_required?
    return if self.flow_type == FlowNode::Flow_Types[:rag]

    if (self.is_source?)
      if (self.source_schema.blank? && !self.data_samples.blank?)
        # Only derive the source_schema if the caller hasn't already
        # set it. Normally the backend will include source_schema when
        # it creates a source data set.
        self.source_schema = TransformService.new.accumulate_schema(self.data_samples, self.org)
      end
      # Transforms are not supported on source data sets.
      self.output_schema = self.source_schema
    elsif (!self.parent_data_set.nil?)
      # Note, calling the :samples method here, not
      # accessing the :data_samples attribute directly.
      # This will try to fetch samples if the parent
      # doesn't already have some cached.
      samples = self.parent_data_set.samples({ :output_only => 1 })
      if (self.empty_transform?)
        self.output_schema = self.parent_data_set.output_schema
        self.data_samples = samples
      elsif !self.skip_schema_detection
        # NEX-6062 limit the number of samples used for schema
        # accumulation if the raw size of samples is greater
        # than one standard deviation from the mean (approx).
        # Also, see NEX-12773 for why we check skip_schema_detection
        samples = samples[0..1] if (samples.to_json.size > Samples_Size_Std_Dev)
        tx = self.apply_transform(samples, true)
        if (tx[:schemas].is_a?(Array) && tx[:schemas][0].is_a?(Hash))
          self.output_schema = tx[:schemas][0]
        end
        self.data_samples = tx[:output] if !tx[:output].blank?
      end
    end

    if (self.output_schema_changed? && !self.output_schema.blank?)
      # NEX-11525 adding a save!() call here because ControlService now
      # calls resource.reload, which wipes out any not-yet-committed
      # changes on the resource. IMPROVE this.
      self.save!
      self.send_control_event(:schema_update)
    end
  end

  def update_output_validator (api_user_info, output_validator_id)
    if (output_validator_id.nil?)
      self.output_validator_id = nil
      return
    end

    v = Validator.find(output_validator_id)
    raise Api::V1::ApiError.new(:bad_request, "Invalid validator code container") if !v.is_validator?
    if (!Ability.new(api_user_info.input_owner).can?(:read, v))
      raise Api::V1::ApiError.new(:forbidden, "Invalid access to validator code container")
    end

    self.output_validator_id = v.id
  end

  def update_transform (request, user, input)
    update_output_schema = false

    if (input.key?(:transform_id))
      # This block MUST come first. :transform_id edits take precedent over :transform
      # edits. This means that any tx content provided in :transform will be applied to
      # the transform record specified by :transform_id (or to a new one if :transform_id
      # is passed as null)
      prev_code_container = self.code_container
      prev_code_container_id = self.code_container&.id

      if (input[:transform_id].nil?)
        if self.flow_type == FlowNode::Flow_Types[:rag] && prev_code_container.present?
          raise Api::V1::ApiError.new(:bad_request, "Invalid code container for AI function node")
        end

        update_output_schema = true
        delete_splitter
        self.code_container_id = nil
        self.save!
        self.reload
      elsif (input[:transform_id] == prev_code_container_id)
        prev_code_container = nil
        prev_code_container_id = nil
      else
        if self.flow_type == FlowNode::Flow_Types[:rag] && prev_code_container.nil?
          raise Api::V1::ApiError.new(:bad_request, "Invalid code container for data retriever")
        end

        update_output_schema = true
        code_container = CodeContainer.find(input[:transform_id])
        if (!Ability.new(user).can?(:read, code_container))
          raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to code container")
        end
        if (!code_container.reusable?)
          raise Api::V1::ApiError.new(:bad_request, "Cannot reuse that code container")
        end
        delete_splitter
        self.transform = code_container
      end
      if (!prev_code_container.nil? && !prev_code_container.reusable?)
        prev_code_container.destroy
      end
    end

    if (input.key?(:transform))
      update_output_schema = true
      self.transform = input[:transform]
    end

    return update_output_schema
  end

  def connector_code
    vendor_code = data_source&.vendor_endpoint&.vendor&.name
    connector_type = self.data_source&.connector_type
    wrap_vendor_parts([vendor_code, connector_type, connection_type])
  end

  def connector_name
    vendor_name = data_source&.vendor_endpoint&.vendor&.display_name
    connector_name = self.data_source&.connector&.name
    wrap_vendor_parts([vendor_name, connector_name, connection_type])
  end

  def connection_type
    self.data_source&.connector&.connection_type
  end

  def splitter_fallback_destination
    return nil if self.transform.blank? || self.transform['transforms'].blank?

    tx = self.transform['transforms'][0]
    return nil if tx['operation'] != SPLITTER_OPERATION

    tx['spec']['fallback_destination']
  end

  private

  def get_schema_properties(data, key_property = nil)
    return [] if data.blank?

    properties = key_property.blank? ? [] : [key_property]

    inside = if data["type"] == "object"
               data["properties"]&.map { |k,v| get_schema_properties(v,k) }
             elsif data["type"] == "array"
               if data["items"].is_a?(Array)
                 ["items"].map { |v| get_schema_properties(v) }
               elsif data.dig("items","anyOf").present?
                 data.dig("items", "anyOf").map { |v| get_schema_properties(v) }
               elsif data.dig("items","properties").present? || data["items"].is_a?(Hash)
                 get_schema_properties data["items"]
               end
             end
    properties += inside.flatten if inside.present?
    properties
  end

  def get_splitter_child_samples(count)
    return [] if count == 0

    parent_splitter = self.parent_data_set
    return [] unless parent_splitter

    transform_success = ->(r) { r && (r[:status] == :ok || (r[:status] == 200 && r[:output].present?)) }

    cached = parent_splitter.data_samples || []
    if cached.present?
      tx = parent_splitter.apply_transform(count > 0 ? cached.first(count) : cached)
      if transform_success.call(tx)
        routed = parent_splitter.send(:get_splitter_data_samples, tx[:output], self.id) || []
        return count > 0 ? routed.first(count) : routed if routed.present?
      end
    end

    grandparent = parent_splitter.parent_data_set
    return [] unless grandparent

    gp_samples = grandparent.get_output_only_samples(count) || []
    return [] if gp_samples.blank?

    tx = parent_splitter.apply_transform(gp_samples)
    return [] unless transform_success.call(tx)

    routed = parent_splitter.send(:get_splitter_data_samples, tx[:output], self.id) || []
    count > 0 ? routed.first(count) : routed
  end

  def get_splitter_transform_output(count)
    return nil unless self.splitter?
    
    input_samples = if self.parent_data_set.present?
      self.parent_data_set.get_output_only_samples(count) || []
    else
      self.data_samples || []
    end
    
    return nil unless input_samples.present?
    
    tx = self.apply_transform(input_samples)
    return nil unless (tx[:status] == :ok || (tx[:status] == 200 && !tx[:output].blank?))
    
    tx[:output]
  end
end