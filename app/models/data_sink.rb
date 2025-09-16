class DataSink < ApplicationRecord
  self.primary_key = :id

  RUNTIME_STATUSES = API_RUNTIME_STATUSES

  include Api::V1::Schema
  include AccessControls::Standard
  include Accessible
  include Copy
  include Chown
  include Docs
  include JsonAccessor
  include FlowNodeData
  include AuditLog
  include PaperTrailAssociations
  include SearchableConcern
  include UpdateRuntimeStatusConcern
  include UpdateAclNotificationConcern
  include DataplaneConcern
  include ReferencedResourcesConcern
  include ConfigDefaultsConcern
  include FlowTriggersConcern
  include ChangeTrackerConcern

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :origin_node, class_name: "FlowNode", foreign_key: "origin_node_id"
  belongs_to :flow_node, dependent: :destroy
  belongs_to :data_credentials
  belongs_to :data_credentials_group, optional: true
  belongs_to :data_map
  belongs_to :data_source
  belongs_to :vendor_endpoint
  belongs_to :code_container
  belongs_to :data_set, association_versions: true
  belongs_to :copied_from, class_name: "DataSink", foreign_key: "copied_from_id"
  belongs_to :connector, foreign_key: "connector_type", primary_key: "type"

  has_many :data_sinks_api_keys, dependent: :destroy
  alias_method :api_keys, :data_sinks_api_keys

  attr_accessor :control_messages_enabled

  acts_as_taggable_on :tags
  def tags_list
    self.tags.map(&:name)
  end
  alias_method :tag_list, :tags_list

  mark_as_referenced_resource
  referencing_resources :data_maps, :data_credentials, :data_sets, :data_sources, :code_containers

  before_create :build_flow_node
  after_create :set_flow_node_sink_id
  before_update :handle_before_update
  after_update :handle_after_update
  before_destroy :handle_before_destroy
  after_destroy :handle_after_destroy
  after_initialize do
    self.control_messages_enabled = true
  end

  after_commit :handle_after_commit_create, on: :create
  after_commit :handle_after_commit_update, on: :update

  json_accessor :sink_config, :template_config

  delegate :flow_type, to: :origin_node, allow_nil: true
  delegate :ingestion_mode, to: :origin_node, allow_nil: true
  delegate :nexset_api_connector_types, to: :ConstantResolver

  Statuses = {
    :init     => 'INIT',
    :paused   => 'PAUSED',
    :active   => 'ACTIVE'
  }

  Script_Data_Credentials_Id = 1
  Create_Data_Source_Key = "create.datasource"
  Ingest_Data_Source_Key = "ingest.datasource.id"

  scope :for_search_index, -> { eager_load(:origin_node, :connector, vendor_endpoint: :vendor) }

  def self.connector_types
    ConstantResolver.instance.api_sink_types
  end

  def self.backend_resource_name
    'sink'.freeze
  end

  def self.all_condensed (filter_opts = {}, sort_opts = {})
    fields = [
      :id, :owner_id, :org_id,
      :origin_node_id, :flow_node_id, :data_set_id,
      :code_container_id, :status, :runtime_status,
      :connector_type, :flow_type, :ingestion_mode,
      :sink_config, :updated_at, :created_at
    ]

    DataSink.joins(:flow_node).where(filter_opts).select(fields).order(sort_opts).preload(:taggings)
  end

  DATA_SET_SELECT = %{
    data_sinks.id, data_sinks.org_id, data_sinks.status, data_sinks.data_set_id,
    data_sets.status as data_set_status,
    data_sets.parent_data_set_id as parent_data_set_ids,
    data_sinks.created_at,data_sinks.updated_at
  }.squish

  DATA_SET_JOIN = %{
    INNER JOIN data_sets
    on data_sets.id = data_sinks.data_set_id
  }.squish

  DATA_SET_SELECT_2 = %{
    data_sinks.id, data_sinks.org_id, data_sinks.status, data_sinks.data_set_id,
    data_sets.status as data_set_status,
    data_sets.parent_data_set_id as parent_data_sets_id,
    data_sinks.created_at,data_sinks.updated_at
  }.squish

  DATA_SET_JOIN_2 = %{
    INNER JOIN data_sets
    on data_sets.id = data_sinks.data_set_id
    WHERE data_sets.data_source_id is not null
  }.squish

  def self.all_by_data_set (filter_status = nil, dataplane = nil)
    data_sinks = DataSink.joins(DATA_SET_JOIN).select(DATA_SET_SELECT)
    data_sinks = data_sinks.union(DataSink.joins(DATA_SET_JOIN_2).select(DATA_SET_SELECT_2))

    if (!filter_status.nil?)
      filter_status = filter_status.upcase
      data_sinks = data_sinks.select {|d| d.data_set_status == filter_status }
    end

    data_sinks = data_sinks.to_a
    if dataplane.present?
      org_ids =  Org.where(cluster_id: dataplane.id).pluck(:id)
      data_sinks = data_sinks.select { |ds| org_ids.include?(ds.org_id) }
    end

    ids_hash = {}

    data_sinks.each do |ds|
      entry = ids_hash[ds.id]
      entry ||= []
      entry << ds.parent_data_set_ids if !ds.parent_data_set_ids.blank?
      ids_hash[ds.id] = entry
    end

    data_sinks = data_sinks.uniq(&:id)
    data_sinks = data_sinks.map {|ds| ds.parent_data_set_ids = ids_hash[ds.id]; ds }

    data_sinks
  end

  def self.default_script_config
    { mapping: { mode: "auto" } }
  end

  def self.build_from_input (api_user_info, input, request = {})
    return nil if (!input.is_a?(Hash) || api_user_info.nil?)
    input.symbolize_keys!

    input[:owner_id] = api_user_info.input_owner.id
    input[:org_id] = (api_user_info.input_org.nil? ? nil : api_user_info.input_org.id)

    data_credentials = input[:data_credentials]
    input.delete(:data_credentials)

    if input.key?(:vendor_endpoint_name)
      vendor_endpoint = VendorEndpoint.find_by_name(input[:vendor_endpoint_name])
      unless vendor_endpoint.nil?
        input[:vendor_endpoint_id] = vendor_endpoint.id
        input.delete(:vendor_endpoint_name)
      end
    end

    map_input = input[:data_map] || {}
    map_input.symbolize_keys!
    input.delete(:data_map)

    if !input[:data_map_id].blank?
      raise Api::V1::ApiError.new(:bad_request, "Cannot create a dynamic map with an existing data map")
    end

    data_sink = nil
    ActiveRecord::Base.transaction do
      if (!data_credentials.nil?)
        # We support either an integer id for an existing
        # credentials resource or an object specifying
        # a new credentials resource to create. Probably
        # should have required callers to pass existing
        # id in 'data_credentials_id' instead, but UI
        # uses 'data_credentials' for both formats.
        if (data_credentials.is_a?(Integer))
          # Note, access permissions for the data_credentials
          # referenced here will be checked in update_mutable!()
          input[:data_credentials_id] = data_credentials
        else
          dc = DataCredentials.new
          dc.set_defaults(api_user_info.input_owner, api_user_info.input_org)
          dc.update_mutable!(api_user_info, data_credentials, {})
          input[:data_credentials_id] = dc.id
        end
      end

      data_sink = DataSink.new
      data_sink.set_defaults(api_user_info.input_owner, api_user_info.input_org)
      data_sink.update_mutable!(api_user_info, input, request)

      # Note, we handle the auto-generated destinations
      # after the save! call in update_mutable because
      # the associated resource, if any, needs the id of
      # the new data sink.

      if (data_sink.connector_type == DataSink.connector_types[:data_map])
        # We do this after the save! in update_mutable() because the
        # destination data_map needs the data_sink association. Also,
        # the data_map can only be created during the data_sink creation.
        data_sink.create_destination_data_map(api_user_info, map_input)
      end

      ResourceTagging.after_create_tagging(data_sink, input, api_user_info.input_owner)
    end

    if !data_sink.valid?
      status = data_sink.status.nil? ? :bad_request : data_sink.status.to_sym
      raise Api::V1::ApiError.new(status, data_sink.errors.full_messages.join(";"))
    end

    return data_sink
  end

  def set_defaults (user, org)
    self.owner = user
    self.org = org
    self.connector = Connector.default_connection_type
  end

  def sink_type
    self.connector_type
  end

  def raw_sink_type (user)
    if (user.is_a?(User) && user.infrastructure_user?)
      return self.connector.connection_type
    end

    self.connector_type
  end

  def update_mutable! (api_user_info, input, request)
    return if (!input.is_a?(Hash) || api_user_info.nil?)
    ability = Ability.new(api_user_info.input_owner)

    if (input.key?(:data_set_id))
      data_set = DataSet.find(input[:data_set_id].to_i)
      if !ability.can?(:manage, data_set)
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data set")
      end
      self.data_set = data_set
    end

    ref_fields = input.delete(:referenced_resource_ids)
    verify_ref_resources!(api_user_info.input_owner, ref_fields)

    tags = input.delete(:tags)

    if input.key?(:vendor_endpoint_name)
      vendor_endpoint = VendorEndpoint.find_by_name(input[:vendor_endpoint_name])
      unless vendor_endpoint.nil?
        input[:vendor_endpoint_id] = vendor_endpoint.id
        input.delete(:vendor_endpoint_name)
      end
    end

    if (!input[:template_config].blank?)
      v = CodeUtils.validate_config(input[:template_config])
      raise Api::V1::ApiError.new(:bad_request, v[:description]) if !v.nil?
    end

    self.name = input[:name] if !input[:name].blank?
    self.description = input[:description] if input.key?(:description)
    self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
    self.org = api_user_info.input_org if (self.org != api_user_info.input_org)

    if input.key?(:sink_type)
      # Convert backwards-compatible key to new key,
      # but preserve new key if it's already in the input.
      input[:connector_type] = input[:sink_type] if !input.key?(:connector_type)
      input.delete(:sink_type)
    end

    connector_type_was = self.connector_type
    if input.key?(:connector_type)
      self.connector = Connector.find_by_type(input[:connector_type])
      raise Api::V1::ApiError.new(:bad_request, "Unknown connector type") if self.connector.nil?
    end

    if (input.key?(:data_credentials_id))
      if (input[:data_credentials_id].nil?)
        dc = nil
      else
        dc = DataCredentials.find(input[:data_credentials_id].to_i)
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data credentials") unless ability.can?(:read, dc)
        raise Api::V1::ApiError.new(:forbidden, "Credentials connector type does not match data sink connector type") if dc.connector_type != self.connector_type
      end
      self.data_credentials = dc
    end

    if (input.key?(:sink_format))
      self.sink_format = DataSource.validate_source_format_str(input[:sink_format])
      raise Api::V1::ApiError.new(:bad_request, "Unknown sink format") if self.sink_format.nil?
    end

    sink_config = input[:sink_config]
    if input.key?(:sink_config)
      if (!sink_config.blank?)
        v = CodeUtils.validate_config(sink_config)
        raise Api::V1::ApiError.new(:bad_request, v[:description]) if !v.nil?
      end
      if (sink_config[Create_Data_Source_Key] && (self.connector_type != DataSink.connector_types[:rest]))
        raise Api::V1::ApiError.new(:bad_request,
          "Config option #{Create_Data_Source_Key} incompatible with connector_type: #{self.connector_type}")
      end

      if sink_config.key?(Create_Data_Source_Key)
        sink_config[Create_Data_Source_Key] = sink_config[Create_Data_Source_Key].truthy?
      end
      self.sink_config = sink_config
    end

    self.apply_config_defaults(api_user_info)
    self.update_template_config(api_user_info.input_owner, input, ability)
    self.sink_schedule = input[:sink_schedule] if input.key?(:sink_schedule)
    self.in_memory = input[:in_memory].truthy? if input.key?(:in_memory)

    # create destination in external system as last step after all the validations and we have set all the attributes
    if (input[:create_destination].truthy? && input.key?(:sink_config))
      input[:sink_config][:dataset_id] = self.data_set.id if self.data_set.present?
      res = ProbeService.new(self).create_destination(input[:sink_config])
      raise Api::V1::ApiError.new(:internal_server_error, res[:message]) if (res[:status] != :ok)
    end

    if input.key?(:data_credentials_group_id)
      if input[:data_credentials_group_id].present?
        group = DataCredentialsGroup.find(input[:data_credentials_group_id])
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data credentials group") unless ability.can?(:read, group)

        if self.data_credentials&.connector_type != group.credentials_type
          raise Api::V1::ApiError.new(:bad_request, "Credentials group must have the same connector type")
        end
      end
      self.data_credentials_group_id = input[:data_credentials_group_id]
    end

    DataSink.transaction do
      self.save!
      # The following updates require that we have valid
      # self.id, so we call them after save!...
      self.update_referenced_resources(ref_fields)
      self.update_script_sink_config(api_user_info.input_owner, input, ability, request)
      self.update_dependent_data_source(api_user_info, connector_type_was)

      if input.key?(:flow_triggers)
        add_flow_triggers(api_user_info, input[:flow_triggers].map(&:symbolize_keys!))
      end

      ResourceTagging.after_create_tagging(self, {tags: tags}, api_user_info.input_owner)
    end
  end

  def apply_sink_config_defaults (api_user_info)
    user_settings_type = UserSettingsType.find_by(name: "sink_config_defaults")
    return unless user_settings_type.present?

    sink_config_defaults = api_user_info.input_owner.user_settings
      .where(org: api_user_info.input_org, user_settings_type_id: user_settings_type.id)
      .first

    if sink_config_defaults.present? && sink_config_defaults.settings[self.connector.type].is_a?(Hash)
      self.sink_config = sink_config_defaults.settings[self.connector.type].merge(self.sink_config)
    end
  end

  def update_dependent_data_source (api_user_info = nil, connector_type_was = nil)
    return if !self.valid?

    self.owner.org = self.org
    api_user_info ||= ApiUserInfo.new(self.owner, self.org)
    connector_type_was ||= self.connector_type

    rest_type = ConstantResolver.instance.api_sink_types[:rest]
    return if (self.connector_type != rest_type) && (connector_type_was != rest_type)

    if (self.connector_type != rest_type)
      need_save = false
      if self.data_source.present?
        self.data_source.data_sink_id = nil
        self.data_source.save!
        self.data_source_id = nil
        need_save = true
      end

      if (self.sink_config.key?(Create_Data_Source_Key))
        cfg = self.sink_config
        cfg.delete(Create_Data_Source_Key)
        cfg.delete(Ingest_Data_Source_Key)
        self.sink_config = cfg
        need_save = true
      end

      self.save! if need_save
      return
    end

    if !self.sink_config.key?(Create_Data_Source_Key) && self.data_source.present?
      cfg = self.sink_config
      if (self.data_source.data_sink_id == self.id)
        # Restore association keys. Caller must explicitly set
        # Create_Data_Source_Key to false to clear the association.
        cfg[Create_Data_Source_Key] = true
        cfg[Ingest_Data_Source_Key] = self.data_source_id
      else
        # The dependent data source was modified to remove
        # the association to this sink.
        cfg.delete(Ingest_Data_Source_Key)
        self.data_source_id = nil
      end
      self.sink_config = cfg
      self.save!
    end

    if !self.sink_config[Create_Data_Source_Key].truthy?
      need_save = false
      if self.data_source_id.present?
        if self.data_source.present?
          self.data_source.data_sink_id = nil
          self.data_source.save!
        end
        self.data_source_id = nil
        need_save = true
      end

      cfg = self.sink_config
      id = cfg.delete(Ingest_Data_Source_Key)
      if (id.present? || !cfg.key?(Create_Data_Source_Key))
        cfg[Create_Data_Source_Key] = false
        self.sink_config = cfg
        need_save = true
      end

      self.save! if need_save
      return
    end

    if !self.data_source.present?
      self.create_dependent_data_source(api_user_info)
      return
    end

    if (self.sink_config[Ingest_Data_Source_Key] != self.data_source_id)
      # Here the caller may have passed in a sink_config without
      # the dependent data source key/value pairs.
      cfg = self.sink_config
      cfg[Ingest_Data_Source_Key] = self.data_source_id
      cfg[Create_Data_Source_Key] = true
      self.sink_config = cfg
      self.save!
    end
  end

  def remove_dependent_data_source
    self.data_source_id = nil
    cfg = self.sink_config
    id = cfg.delete(Ingest_Data_Source_Key)
    self.sink_config = cfg if id.present?
    self.save!
  end

  def create_dependent_data_source (api_user_info)
    if (self.connector_type != DataSink.connector_types[:rest])
      # We currently support auto-creation of downstream
      # sources only for 'rest' sinks writing to webhook sources.
      raise Api::V1::ApiError.new(:internal_server_error,
        "Invalid connector type for ingestion data source")
    end

    input = {
      :owner_id => self.owner.id,
      :org_id => self.org.present? ? self.org.id : nil,
      :name => "Request responses from: #{self.name}",
      :connector_type => DataSource.connector_types[:nexla_rest],
      :data_sink_id => self.id,
      :source_config => {
        DataSource::Single_Schema_Key => true
      }
    }

    data_source = DataSource.build_from_input(api_user_info, input)
    if !data_source.valid?
      status = data_source.status.nil? ? :bad_request : data_source.status.to_sym
      raise Api::V1::ApiError.new(status, data_source.errors.full_messages.join(";"))
    end

    cfg = self.sink_config
    cfg[Ingest_Data_Source_Key] = data_source.id
    cfg[Create_Data_Source_Key] = true

    self.sink_config = cfg
    self.data_source_id = data_source.id
    self.save!
  end

  def create_destination_data_map (api_user_info, map_input)
    if (self.connector_type != DataSink.connector_types[:data_map])
      raise Api::V1::ApiError.new(:internal_server_error,
        "Invalid connector type for data map")
    end

    map_input[:name]      ||= self.name
    map_input[:data_type] ||= "string"

    DataMap.validate_input_schema(map_input, :post)

    map_input[:owner_id] = api_user_info.input_owner.id
    map_input[:org_id] = (api_user_info.input_org.nil? ? nil : api_user_info.input_org.id)

    data_map = DataMap.new(map_input)
    data_map.data_sink_id = self.id
    data_map.use_versioning = false
    data_map.save!

    if (self.data_credentials_id.nil?)
      # NEX-678 associate empty credentials with data_sink
      # that writes to a dynamic data_map
      dc = DataCredentials.where(
        :owner => api_user_info.input_owner,
        :org => api_user_info.input_org,
        :connector_type => DataSink.connector_types[:data_map]
      ).first
      if dc.nil?
        dc = DataCredentials.new
        dc.set_defaults(api_user_info.input_owner, api_user_info.input_org)

        dc.update_mutable!(api_user_info, {
          :connector => Connector.find_by_type(ConstantResolver.instance.api_sink_types[:data_map]),
          :credentials => {
            :credentials_type => ConstantResolver.instance.api_sink_types[:data_map]
          }
        }, {})
      end
      self.data_credentials_id = dc.id
    end

    self.data_map_id = data_map.id
    self.save!
  end

  def build_api_key_from_input (api_user_info, input)
    api_key = DataSinksApiKey.new
    api_key.owner = self.owner
    api_key.org = self.org
    api_key.data_sink_id = self.id
    api_key.update_mutable!(api_user_info, input)
    return api_key
  end

  def copy_pre_save (original_data_sink, api_user_info, options = {})
    self.flow_node = self.flow_node.copy(api_user_info, options)
    self.origin_node_id = self.flow_node.origin_node_id
    self.data_source_id = nil
    cfg = self.sink_config
    cfg.delete(Ingest_Data_Source_Key)
    cfg.delete(Create_Data_Source_Key)
    self.sink_config = cfg
  end

  def copy_post_save (original_data_sink, api_user_info, options = {})
    self.flow_node.data_sink_id = self.id
    self.flow_node.save!

    if data_map && data_map.data_sink_id != self.id
      data_map.update(data_sink_id: self.id)
    end
  end

  def update_template_config (user, input, ability)
    return if !VendorEndpoint.valid_template_config_parameters?(input)

    self.template_config = input[:template_config]
    sink_config, vendor_endpoint = VendorEndpoint.get_resource_config(self.data_credentials,
      input, 'SINK', input[:template_config], self.id)
    self.sink_config = sink_config
    self.vendor_endpoint_id = vendor_endpoint.id
    self.connector = Connector.find_by_type(vendor_endpoint.connection_type)
  end

  def update_script_sink_config (user, input, ability, request = nil)
    return if (!VendorEndpoint.valid_script_config_parameters?(input) || self.id.nil?)

    code_container = vendor_endpoint = nil

    if (input.key?(:script_config_id))
      code_container = CodeContainer.find(input[:script_config_id])
      if (!ability.can?(:read, code_container))
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to code container")
      end
      if (!code_container.reusable?)
        raise Api::V1::ApiError.new(:bad_request, "Cannot reuse that code container")
      end
      vendor_endpoint = input.key?(:vendor_endpoint_name) ? VendorEndpoint.find_by_name(input[:vendor_endpoint_name]) :
        VendorEndpoint.find(input[:vendor_endpoint_id])
      raise Api::V1::ApiError.new(:bad_request, "Invalid vendor endpoint") if vendor_endpoint.nil?
    elsif (input.key?(:script_config) && !input[:script_config]["config"].nil?)
      script_config, vendor_endpoint = VendorEndpoint.get_resource_config(self.data_credentials, input, 'SINK', input[:script_config]["config"], self.id)
      code_container = CodeContainer.create_resource_script_config(input, script_config, self, user, self.org, CodeContainer::Resource_Types[:sink])
    end

    self.code_container_id = code_container.id if !code_container.nil?
    if (!vendor_endpoint.nil?)
      self.vendor_endpoint_id = vendor_endpoint.id
      endpoint_connector_type = DataSink.connector_types.key(vendor_endpoint.connection_type)
      if endpoint_connector_type.present?
        self.connector = Connector.find_by_type(endpoint_connector_type)
      elsif (!input.key?(:sink_type) && !input.key?(:connector_type))
        self.connector = Connector.default_connection_type
      end
    end

    if !input.key?(:sink_config)
      self.sink_config = self.script_sink_config(request)
    elsif input.key?(:script_config)
      self.sink_config = self.script_sink_config(request).merge(input[:sink_config])
    end
    self.save!
  end

  def encrypted_credentials
    self.data_credentials.encrypted_credentials if self.data_credentials.present?
  end

  def has_credentials?
    !self.data_credentials.nil? && !self.data_credentials.credentials_enc.blank?
  end

  def active?
    self.status == Statuses[:active]
  end

  def paused?
    self.status == Statuses[:paused]
  end

  def activate!
    return if self.active?
    self.update_dependent_data_source if (self.connector_type == DataSink.connector_types[:rest])

    Notifications::ResourceNotifier.exclusive_for(self) do
      self.data_source.activate! if self.data_source.present?
      self.transaction do
        self.status = Statuses[:active]
        self.flow_node.status = self.status
        self.flow_node.save!
        self.save!
        self.reload
      end

      if self.active?
        self.send_control_event(:activate) if self.active?
        Notifications::ResourceNotifier.new(self, :activate).call
        update_flow_ingestion_mode
      end
    end
  end

  def pause!
    return if self.paused?

    Notifications::ResourceNotifier.exclusive_for(self) do
      self.data_source.pause! if self.data_source.present?
      self.transaction do
        self.status = Statuses[:paused]
        self.flow_node.status = self.status
        self.flow_node.save!
        self.save!
        self.reload
      end
      if self.paused?
        self.send_control_event(:pause) if self.paused?
        Notifications::ResourceNotifier.new(self, :pause).call
      end
    end
  end

  def destroy
    # If already destroyed, return. Called by FlowNode.destroy as association.
    return self if self.id.nil? || self.class.find_by(id: self.id).nil?

    raise Api::V1::ApiError.new(:method_not_allowed, "Data sink must be paused before deletion") if self.active?
    self.data_source.remove_origin_data_sink if self.data_source.present?

    # Reload origin FlowNode so its reference is located in-memory during destroy callback cycle of isolated sinks.
    self.origin_node&.reload if self.isolated?

    super

    Notifications::ResourceNotifier.new(self, :delete).call
  end

  def isolated?
    self.data_set_id.blank? && self.origin_node_id == self.flow_node_id
  end

  def has_template?
    (!(self.vendor_endpoint_id.nil?) && !self.vendor_endpoint.nil?)
  end

  def vendor_id
    (!self.vendor_endpoint.nil? && !self.vendor_endpoint.vendor.nil?) ? self.vendor_endpoint.vendor.id : nil
  end

  def vendor_info
    v = nil
    if (self.vendor_endpoint.present? && self.vendor_endpoint.vendor.present?)
      v = {
        :id => self.vendor_endpoint.vendor.id,
        :name => self.vendor_endpoint.vendor.name
      }
    end
    return v
  end

  def vendor_code
    vendor_code = vendor_endpoint&.vendor&.name
    connector_type = self.connector_type
    wrap_vendor_parts([vendor_code, connector_type, connection_type])
  end

  def vendor_display_name
    vendor_name = vendor_endpoint&.vendor&.display_name
    connector_name = self.connector&.name
    wrap_vendor_parts([vendor_name, connector_name, connection_type])
  end

  def connection_type
    self.connector&.connection_type
  end

  def flow_attributes (user, org)
    [
      :data_credentials_id,
      :data_credentials_group_id,
      :data_set_id,
      :data_source_id,
      :managed,
      :ingestion_mode,
      :sink_type,
      :connector_type,
      :connection_type,
      :template_config,
      :sink_config,
      :vendor
    ].map do |attr|
      case attr
      when :connection_type, :sink_type
        [ attr, self.raw_sink_type(user) ]
      when :vendor
        [ attr, self.vendor_info ]
      else
        [ attr, self.send(attr) ]
      end
    end
  end

  def flows (downstream_only = false, user = nil, org = nil, admin_level = :none)
    if (self.data_set.nil?)
      flows = DataFlow.empty_flows
      flows[:data_sinks] << self.flow_resource_data(flows, user, org)
      return flows
    end
    return self.data_set.flows(downstream_only, user, org, admin_level)
  end

  def flow_origin (user = nil, org = nil)
    # Note, flow origin may be either a data source
    # or a data set, depending on whether there is a
    # shared data set upstream that crosses an org
    # boundary. If the caller is a Nexla admin and
    # you need the flow source, use flow_source().
    return nil if self.data_set.nil?
    if (user.nil?)
      user = self.owner
      user.org = org = self.org
    end
    return self.data_set.flow_origin(user, org)
  end

  def flow_source (user)
    return nil if self.data_set.nil?
    if !user.super_user?
      raise Api::V1::ApiError.new(:internal_server_error, "Invalid caller for flow_source")
    end

    src = nil
    ds = self.data_set

    while (ds.present?) do
      if (ds.is_source?)
        src = DataSource.find_by_id(ds.data_source_id)
        break
      end
      ds = ds.parent_data_set
    end

    return src
  end

  def script_enabled?
    (self.connector_type == DataSink.connector_types[:script] || script_vendor? || !self.code_container.nil?)
  end

  def script_vendor?
    (!self.vendor_endpoint.nil? && !self.vendor_endpoint.vendor.nil? && self.vendor_endpoint.vendor.connection_type == DataSink.connector_types[:script])
  end

  def script_data_credentials
    DataCredentials.find_by_id(Script_Data_Credentials_Id)
  end

  def script_sink_config (request)
    cluster_config = ClusterScriptConfig.config(self, request)
    sink_config =  DataSink.default_script_config

    cluster_config.each do |key, value|
      case key
      when :prod_ui_host, :path_prefix, :env_name
        next
      when :mapping_mode
        sink_config[:mapping][:mode] = value
      when :mapping_tracker_mode
        sink_config[:mapping][:tracker_mode] = value
      when :path
        sink_config[:path] = value +
          "/#{cluster_config[:path_prefix]}/#{cluster_config[:env_name]}/#{self.id}"
      else
        sink_config[key] = value
      end
    end

    sink_config
  end

  def searchable_attributes
    vendor_code = self.vendor_code
    vendor_name = self.vendor_display_name
    attrs =  {
      data_sink_id: self.id,
      sink_type: self.sink_type,
      connector_code: vendor_code,
      connector_name: vendor_name,
      sink_connector_code: vendor_code,
      sink_connector_name: vendor_name,
      project_id: origin_node&.project_id,
      nexset_api_compatible: origin_node&.nexset_api_compatible,
      flow_type: origin_node&.flow_type,
      rag: (origin_node&.flow_type == FlowNode::Flow_Types[:rag])
    }
    self.attributes.merge(attrs)
  end

  def run_status (api_user_info, run_id)
    ds = self.origin_node.resource
    if !ds.is_a?(DataSource)
      raise Api::V1::ApiError.new(:bad_request,
        "Flow origin for data sink #{self.id} is not a data source")
    end
    if !Ability.new(api_user_info.user).can?(:read, ds)
      raise Api::V1::ApiError.new(:forbidden)
    end

    run_id = (run_id || "").downcase == "latest" ? "" : run_id
    run = ds.run_ids.where(run_id.present? ? { run_id: run_id } : nil).first
    if run.blank?
      raise Api::V1::ApiError.new(:not_found,
        run_id.blank? ? "No runs found for data sink #{self.id}" :
          "Run not found for: #{run_id}")
    end

    return ControlService.new(self).get_run_status(self.id, run.run_id)
  end

  def send_control_event (event_type)
    return if flow_type == FlowNode::Flow_Types[:rag]

    ControlService.new(self).publish(event_type) if self.control_messages_enabled
  end

  def node_type
    case self.flow_type
    when FlowNode::Flow_Types[:rag]
      :llm_processor
    when FlowNode::Flow_Types[:api_server]
      :api_target
    else
      :data_sink
    end
  end

  def adaptive_flow?
    data_source = self.origin_node&.data_source
    return false if data_source.blank?

    data_source.adaptive_flow?
  end

  def run_variables
    return [] unless self.adaptive_flow?

    extract_all_params(self.sink_config).uniq { |param| param[:param] }
  end

  def extract_all_params(parameters)
    case parameters
    when Array then parameters.flat_map { |element| extract_all_params(element) }
    when Hash then parameters.values.flat_map { |element| extract_all_params(element) }
    when String then extract_params(parameters)
    else
      []
    end
  end

  def extract_params(template)
    params = Array.new
    return params if !template.is_a?(String)
    matches = TemplateVariableExtractor.new(template).get
    matches.each do |m|
      next if m.blank?
      param = m.split("=", 2)
      if (param.size == 1)
        params << { :param => param[0].strip }
      elsif (param.size > 1)
        params << { :param => param[0].strip, :default => param[1] }
      end
    end
    return params
  end

  protected

  def disable_control_messages
    self.control_messages_enabled = false
  end

  def handle_after_commit_create
    self.send_control_event(:create)
  end

  def handle_after_commit_update
    self.send_control_event(:update)
  end

  def handle_before_update
    if self.will_save_change_to_data_set_id?
      if (self.data_set_id.present? && self.data_set.present?)
        if !self.data_set.flow_node.present?
          raise Api::V1::ApiError.new(:internal_server_error,
            "Missing parent flow node for data sink #{self.if}: #{self.data_set.id}")
        end
        FlowNode.reset_flow_origin(self, self.data_set.flow_node)
      else
        FlowNode.reset_flow_origin(self, nil)
      end
    end
  end

  def handle_before_destroy
    self.flow_triggers.destroy_all
  end

  def handle_after_destroy
    if self.data_source.present?
      # Note, passing flow_node_id here, not origin_node_id, because
      # we are only deleting the flow downstream from (and including)
      # the dependent data source.
      FlowDeleteWorker.perform_async_with_audit_log(self.data_source.flow_node_id)
    end
    self.send_control_event(:delete)
  end

  def handle_after_update
    if (self.saved_change_to_owner_id? || self.saved_change_to_org_id?)
      self.api_keys.each(&:save!)
      self.flow_node.owner = self.owner
      self.flow_node.org = self.org
      self.flow_node.save!
    end
  end

  def set_flow_node_sink_id
    self.flow_node.data_sink_id = self.id
    self.flow_node.save!

    if self.data_set_id.present? && self.flow_type == FlowNode::Flow_Types[:api_server]
      api_user_info = ApiUserInfo.new(self.owner, self.org)

      DataSet.build_from_input(
        api_user_info,
        {
          name: "Result",
          description: "Result for target #{self.id}",
          parent_data_sink_id: self.id,
          runtime_config: { node_type: :api_result }
        }
      )

      DataSet.build_from_input(
        api_user_info,
        {
          name: "Error",
          description: "Error for target #{self.id}",
          parent_data_sink_id: self.id,
          runtime_config: { node_type: :api_error }
        }
      )
    end
  end

  def build_flow_node
    return if self.flow_node.present?
    pn_id = nil
    on_id = nil

    if self.data_set.present?
      if !self.data_set.flow_node.present?
        raise Api::V1::ApiError.new(:internal_server_error,
          "Missing parent flow node for data sink: #{self.data_set.id}")
      end
      pn_id = self.data_set.flow_node.id
      on_id = self.data_set.flow_node.origin_node_id
    end

    flow_node = FlowNode.new({
      owner_id: self.owner_id,
      org_id: self.org_id,
      data_sink_id: self.id,
      parent_node_id: pn_id,
      origin_node_id: on_id,
      name: self.flow_name.blank? ?
        self.name : self.flow_name,
      description: self.flow_description.blank? ?
        self.description : self.flow_description,
      status: self.status,
      managed: self.managed
    })

    if self.in_memory?
      flow_node.flow_type = FlowNode::Flow_Types[:in_memory]
    end

    # Note, no self.save! here. This method is
    # is called from before_create handler. But
    # we do save the flow_node.
    flow_node.save!

    if flow_node.origin_node_id.nil?
      # Can we avoid this double-save on origin flow_nodes?
      # Note, this is an isolated data_sink with no
      # data_set association.
      flow_node.parent_node_id = nil
      flow_node.origin_node_id = flow_node.id
      flow_node.save!
    end

    self.flow_node_id = flow_node.id
    self.origin_node_id = flow_node.origin_node_id
  end

  def update_flow_ingestion_mode
    if self.origin_node.data_source_id && self.origin_node.ingestion_mode.to_s != 'full_ingestion'
      self.origin_node&.update(ingestion_mode: :full_ingestion)
      result = ControlService.new(data_set.origin_node).update_ingestion_mode

      source = self.origin_node.data_source
      source.send_control_event(:update) if source.present?

      result
    end
  end
end
