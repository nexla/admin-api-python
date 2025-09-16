require 'securerandom'

class DataSource < ApplicationRecord
  self.primary_key = :id

  RUNTIME_STATUSES = API_RUNTIME_STATUSES

  include Api::V1::Schema
  include AccessControls::Standard
  include Accessible
  include JsonAccessor
  include Summary
  include FlowNodeData
  include AuditLog
  include Copy
  include Chown
  include Docs
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
  belongs_to :data_sink
  belongs_to :code_container
  belongs_to :vendor_endpoint
  belongs_to :copied_from, class_name: "DataSource", foreign_key: "copied_from_id"
  belongs_to :connector, foreign_key: "connector_type", primary_key: "type"

  has_many :data_sets
  has_many :data_sources_run_ids, -> { order(:created_at => :desc) }, dependent: :destroy
  alias_method :run_ids, :data_sources_run_ids
  has_many :data_sources_api_keys, dependent: :destroy
  has_many :service_keys, dependent: :destroy
  has_many :endpoint_mappings, dependent: :destroy

  attr_accessor :control_messages_enabled
  attr_accessor :parent_data_set_id

  acts_as_taggable_on :tags
  def tags_list
    self.tags.pluck(:name)
  end
  alias_method :tag_list, :tags_list

  # Note, AccessControls module automatically adds has_many
  # for :data_sources_access_controls, with alias as :access_controls

  json_accessor :source_config, :template_config

  before_create :build_flow_node
  after_create :handle_after_create
  after_commit :handle_after_create_commit, on: :create
  after_commit :handle_after_update_commit, on: :update

  before_update :handle_before_update
  after_update :handle_after_update
  before_destroy :handle_before_destroy
  after_destroy :handle_after_destroy

  after_initialize do
    self.control_messages_enabled = true
  end

  delegate :flow_type, to: :origin_node, allow_nil: true
  delegate :ingestion_mode, to: :origin_node, allow_nil: true

  Default_Run_Id_Count = 5

  mark_as_referenced_resource
  referencing_resources :data_maps, :data_credentials, :data_sets, :data_sinks, :code_containers, :runtimes

  Ingest_Methods = {
    :poll  => 'POLL',
    :api   => 'API'
  }

  Statuses = API_DATA_SOURCE_STATUSES

  Script_Data_Credentials_Id = 1
  Single_Schema_Key = "schema.detection.once"
  Pipeline_Type_Key = "pipeline.type"

  Source_Formats = {
    :unknown   => 'unknown',
    :json      => 'JSON',
    :csv       => 'CSV',
    :tsv       => 'TSV',
    :xml       => 'XML'
  }

  scope :for_search_index, -> { eager_load(:origin_node, :connector, vendor_endpoint: :vendor) }

  def self.connector_types
    ConstantResolver.instance.api_source_types
  end

  def self.validate_source_format_str (fmt_str)
    return nil if fmt_str.class != String
    return Source_Formats[:unknown] if Source_Formats.find { |sym, str| str == fmt_str }.nil?
    fmt_str
  end

  # Note below: connection_type selected as connection_type_raw
  # to avoid being overridden by the DataSource::connection_type
  # method, which would cause an additional join for each source.
  # Similarly for flow_type and ingestion_mode, they are selected
  # as flow_type_raw and ingestion_mode_raw respectively to
  # avoid being overridden by the DataSource::flow_type and
  # DataSource::ingestion_mode methods, which would cause an
  # additional join for each source.

  CONDENSED_SELECT = %{
    data_sources.id, data_sources.owner_id, data_sources.org_id,
    data_sources.origin_node_id, data_sources.flow_node_id,
    data_sources.status, data_sources.runtime_status,
    data_sources.source_config,
    data_sources.updated_at, data_sources.created_at,
    data_sources.data_credentials_id,
    data_sources.connector_type,
    data_credentials.credentials_enc,
    data_credentials.credentials_enc_iv,
    code_containers.code_config,
    code_containers.id as code_container_id,
    connectors.connection_type as connection_type_raw,
    orgs.cluster_id, orgs.new_cluster_id, orgs.cluster_status,
    flow_nodes.flow_type as flow_type_raw,
    flow_nodes.ingestion_mode as ingestion_mode_raw
  }.squish

  CONDENSED_JOIN = %{
    LEFT OUTER JOIN data_credentials
    on data_credentials.id = data_sources.data_credentials_id
    LEFT OUTER JOIN code_containers
    on code_containers.id = data_sources.code_container_id
    LEFT OUTER JOIN connectors
    on connectors.type = data_sources.connector_type
    LEFT OUTER JOIN orgs
    on orgs.id = data_sources.org_id
    LEFT OUTER JOIN flow_nodes
    on flow_nodes.id = data_sources.origin_node_id
  }.squish

  def self.backend_resource_name
    'source'.freeze
  end

  def self.all_condensed (filter_opts = {}, sort_opts = {})
    DataSource.joins(CONDENSED_JOIN).distinct.where(filter_opts).select(CONDENSED_SELECT).order(sort_opts)
  end

  def self.default_script_config (data_source, request)
    cluster_config = ClusterScriptConfig.config(data_source, request)
    script_config = {
      "config" => {
        "host" => ClusterScriptConfig.host(request),
        "meta_dir" => cluster_config[:path] +
          "/#{cluster_config[:path_prefix]}/#{cluster_config[:env_name]}"
      }
    }
    script_config
  end

  def self.build_from_input (api_user_info, input, request = {})
    return nil if (!input.is_a?(Hash) || api_user_info.nil?)
    input.symbolize_keys!

    activate_now = input[:activate_now]

    data_credentials = input[:data_credentials]
    input.delete(:data_credentials)

    if (!data_credentials.nil?)
      if (data_credentials.is_a?(Integer))
        input[:data_credentials_id] = data_credentials
        # Note, access permissions for the data_credentials
        # referenced here will be checked in update_mutable!()
      else
        # Here input[:data_credentials] is expected to
        # be a valid data_credentials input hash.
        dc = DataCredentials.new
        dc.set_defaults(api_user_info.input_owner, api_user_info.input_org)
        dc.update_mutable!(api_user_info, data_credentials, {})
        input[:data_credentials_id] = dc.id
      end
    end

    if input.key?(:vendor_endpoint_name)
      vendor_endpoint = VendorEndpoint.find_by_name(input[:vendor_endpoint_name])
      unless vendor_endpoint.nil?
        input[:vendor_endpoint_id] = vendor_endpoint.id
        input.delete(:vendor_endpoint_name)
      end
    end

    data_source = DataSource.new
    data_source.set_defaults(api_user_info.input_owner, api_user_info.input_org)

    if (input.key?(:data_sink_id))
      data_sink = DataSink.find(input[:data_sink_id].to_i)
      ability = Ability.new(api_user_info.input_owner)
      if (!ability.can?(:manage, data_sink))
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data sink")
      end
      data_source.data_sink_id = input[:data_sink_id]
    end

    DataSource.transaction do
      data_source.update_mutable!(api_user_info, input, request)

      if !(data_source.valid?)
        status = data_source.status.nil? ? :bad_request : data_source.status.to_sym
        raise Api::V1::ApiError.new(status, data_source.errors.full_messages.join(";"))
      end

      data_source.flow_node.update(ingestion_mode: data_source.connector.ingestion_mode) if data_source.flow_node.present?

      data_source.activate!(true, false, run_now: false) if activate_now.truthy?
   end

    return data_source
  end

  def set_defaults (user, org)
    self.owner           = user
    self.org             = org
    self.status          ||= Statuses[:init]
    self.ingest_method   ||= Ingest_Methods[:api]
    self.source_format   ||= Source_Formats[:json]
    self.connector       = Connector.default_connection_type
  end

  def source_type
    self.connector_type
  end

  def raw_source_type (user)
    if (user.is_a?(User) && user.infrastructure_user?)
      return self.connector.connection_type
    end

    self.connector_type
  end

  def update_mutable! (api_user_info, input, request, force = false, run_now: nil)
    return if input.nil? || api_user_info.nil?

    # Allow to edit forced attributes if the run_now is passed
    force ||= !run_now.nil?
    forbid_edit_activated = self.active? && !run_now.nil?

    DataSource.transaction do
      ability = Ability.new(api_user_info.input_owner)

      tags = input.delete(:tags)
      ref_fields = input.delete(:referenced_resource_ids)
      self.verify_ref_resources!(api_user_info.input_owner, ref_fields)

      self.name = input[:name] if !input[:name].blank?
      self.description = input[:description] if input.key?(:description)
      self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
      self.org = api_user_info.input_org if (self.org != api_user_info.input_org)

      if (input.key?(:code_container_id))
        if (input[:code_container_id].nil?)
          code_container = nil
        else
          code_container = CodeContainer.find(input[:code_container_id].to_i)
          if (!ability.can?(:read, code_container))
            raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to code container")
          end
          unless ([CodeContainer::Resource_Types[:source], CodeContainer::Resource_Types[:source_custom]].include?(code_container.resource_type))
            raise Api::V1::ApiError.new(:bad_request, "Invalid code container resource type")
          end
        end
        self.code_container = code_container
      end

      if (input.key?(:data_credentials_id))
        data_credentials = DataCredentials.find(input[:data_credentials_id].to_i)
        if (!ability.can?(:read, data_credentials))
          raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data credentials")
        end
        if data_credentials.org_id != self.org_id
          raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data credentials")
        end
        self.data_credentials = data_credentials
      end

      if input.key?(:source_type)
        # Convert backwards-compatible key to new key,
        # but preserve new key if it's already in the input.
        input[:connector_type] = input[:source_type] if !input.key?(:connector_type)
        input.delete(:source_type)
      end

      if (input.key?(:connector_type))
        raise Api::V1::ApiError.new(:method_not_allowed, "Can't change connector type of active source") if forbid_edit_activated
        if DataSource.connector_types.key(input[:connector_type]).nil?
          raise Api::V1::ApiError.new(:bad_request, "Unsupported source connector type")
        end
        self.connector = Connector.find_by_type(input[:connector_type])
        raise Api::V1::ApiError.new(:bad_request, "Unknown source connector type") if self.connector.nil?
      end

      if (input.key?(:ingest_method))
        raise Api::V1::ApiError.new(:method_not_allowed, "Can't change ingest method of active source") if forbid_edit_activated
        im = input[:ingest_method]
        raise Api::V1::ApiError.new(:bad_request, "Unknown ingest method") if Ingest_Methods.find{|k,v| im == v}.nil?
        self.ingest_method = im
      end

      if (input.key?(:source_format))
        # Note, normally source_format is detected by Ingestion. It's only useful to set
        # it manually through the API to give Ingestion a hint, or the caller is setting up
        # a fixed-config data source with an expected data format.
        if (self.active? && !force)
          raise Api::V1::ApiError.new(:method_not_allowed, "Can't change format of active source")
        end
        self.source_format = DataSource.validate_source_format_str(input[:source_format])
        raise Api::V1::ApiError.new(:bad_request, "Unknown source format") if self.source_format.nil?
      end

      if input.key?(:vendor_endpoint_name)
        vendor_endpoint = VendorEndpoint.find_by_name!(input[:vendor_endpoint_name])
        unless vendor_endpoint.nil?
          input[:vendor_endpoint_id] = vendor_endpoint.id
          input.delete(:vendor_endpoint_name)
        end
      end

      source_config_present = input[:source_config].present?

      if (input.key?(:flow_type))
        # Note, as of api-3.4 we support setting the flow type in two ways:
        # 1) by including a flow_type attribute in the input, and 2) by setting
        # pipeline.type in source_config. We will eventually remove support
        # for option 2.
        ft = FlowNode.validate_flow_type(input[:flow_type])
        raise Api::V1::ApiError.new(:bad_request, "Invalid flow_type attribute") if ft.nil?
        input[:source_config] ||= Hash.new
        # Note, this will override the Pipeline_Type_Key value if
        # it was passed in the input source_config object.
        # :flow_type attribute takes precedence.
        input[:source_config][Pipeline_Type_Key] = ft
        # Also note: we let the input.key?(:source_config) block below
        # handle the updating of self.source_config and self.flow_node.flow_type,
        # to avoid repeating the logic. Move it up here when Pipeline_Type_Key
        # is phased out.
      end

      if (input.key?(:source_config))
        if (self.active? && !force)
          raise Api::V1::ApiError.new(:method_not_allowed, "Cannot change source_config of active data_source")
        end
        v = CodeUtils.validate_config(input[:source_config])
        raise Api::V1::ApiError.new(:bad_request, v[:description]) if !v.nil?

        new_flow_type = nil
        if input[:source_config].key?(Pipeline_Type_Key)
          new_flow_type = FlowNode.validate_flow_type(input[:source_config][Pipeline_Type_Key])
          raise Api::V1::ApiError.new(:bad_request, "Invalid flow type in source config") if new_flow_type.nil?
        else
          new_flow_type = FlowNode.default_flow_type
        end

        # Note, we don't store pipeline.type in source_config if the
        # type is the default (streaming).
        self.source_config = (new_flow_type == FlowNode.default_flow_type) ?
          input[:source_config].except(Pipeline_Type_Key) : input[:source_config]

        if (self.flow_node.present? && (self.flow_node.flow_type != new_flow_type))
          self.flow_node.update(flow_type: new_flow_type)
        end
      end

      if !source_config_present && VendorEndpoint.valid_template_config_parameters?(input)
        if (self.active? && !force)
          raise Api::V1::ApiError.new(:method_not_allowed, "Can't change template_config of active source")
        end
        v = CodeUtils.validate_config(input[:template_config])
        raise Api::V1::ApiError.new(:bad_request, v[:description]) if !v.nil?
        self.template_config = input[:template_config]
        source_config, vendor_endpoint = VendorEndpoint.get_resource_config(self.data_credentials, input, 'SOURCE', input[:template_config], self.id)

        self.source_config ||= {}
        self.source_config = self.source_config.merge(source_config)
        self.vendor_endpoint_id = vendor_endpoint.id
        self.connector = Connector.find_by_type(vendor_endpoint.connection_type)
      end

      if input.key?(:adaptive_flow)
        self.source_config ||= {}
        if input[:adaptive_flow].truthy?
          raise Api::V1::ApiError.new(:bad_request, "External triggers are only available for DirectFlow") if
            (self.flow_node.blank? && self.source_config[Pipeline_Type_Key] != FlowNode::Flow_Types[:in_memory]) ||
            (self.flow_node.present? && self.flow_type != FlowNode::Flow_Types[:in_memory])

          self.adaptive_flow = true
          self.source_config = self.source_config.merge({ 'adaptive.flow' => true })
        else
          self.adaptive_flow = false
          self.source_config = self.source_config.except('adaptive.flow')
        end
      end

      if input[:ingestion_mode].present? && self.flow_node.present?
        self.flow_node.update(ingestion_mode: input[:ingestion_mode])
      end

      self.apply_config_defaults(api_user_info)
      self.poll_schedule = input[:poll_schedule] if input.key?(:poll_schedule)

      if input[:code_container].present? && input[:code_container_id].blank?
        code_container_input = input.delete(:code_container)

        code_container = CodeContainer.build_from_input(api_user_info, code_container_input)
        self.code_container_id = code_container.id
      end

      self.last_run_id = input[:run_id] if input.key?(:run_id)

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

      self.parent_data_set_id = input[:parent_data_set_id] if input.key?(:parent_data_set_id)

      self.save!

      if input.key?(:project_id)
        if input[:project_id].present?
          project = Project.find_by(id: input[:project_id])
          raise Api::V1::ApiError.new(:not_found, "Project not found") if !project || project.org_id != self.org_id
          raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to project") if !ability.can?(:read, project)
          self.flow_node.update(project: project)
        else
          self.flow_node.update(project: nil)
        end
      end

      run_now! if run_now.truthy?

      prev_code_container = self.code_container_id_was && CodeContainer.find_by_id(self.code_container_id_was)
      if self.code_container_id_changed?
        if prev_code_container.nil? && !prev_code_container.reusable?
          prev_code_container.destroy
        end
      end

      if input.key?(:flow_triggers)
        add_flow_triggers(api_user_info, input[:flow_triggers].map(&:symbolize_keys!))
      end

      self.update_referenced_resources(ref_fields)

      # We make these updates after the save!() call because
      # they require self.id to be set, which doesn't happen
      # in the create path until we do a save.
      update_script_source_config(api_user_info.input_owner, input, request)
      if input[:run_id].present?
        self.transaction do
          # We receive PUT /data_sources/{id} with run_id in request body some
          # time after PUT /data_sources/{id}/activate, which is also saving
          # the run_id (if it gets one). So we have to check here to avoid
          # double-saving the same run_id.
          self.run_ids.where(run_id: input[:run_id]).first_or_create
        end
      end

      ResourceTagging.add_owned_tags(self, { tags: tags }, api_user_info.input_owner)
    end
  end

  def copy_pre_save (original_data_source, api_user_info, options)
    prev_flow_node = self.flow_node
    self.flow_node = self.flow_node.copy(api_user_info, options)
    self.origin_node_id = self.flow_node.origin_node_id
  end

  def copy_post_save (original_data_source, api_user_info, options)
    self.transaction do
      self.flow_node.data_source_id = self.id
      self.flow_node.ingestion_mode = ingestion_mode_from_connector
      self.flow_node.save!
      unless options.key?(:copy_flow_triggers) && !options[:copy_flow_triggers]
        original_data_source.flow_triggers.each do |flow_trigger|
          ft = flow_trigger.dup
          ft.triggered_origin_node_id = self.origin_node_id
          ft.save!
        end
      end
    end
  end

  def runs (all = false)
    all ? self.run_ids : self.run_ids.first(Default_Run_Id_Count)
  end

  def build_api_key_from_input (api_user_info, input)
    api_key = DataSourcesApiKey.new
    api_key.owner = self.owner
    api_key.org = self.org
    api_key.data_source_id = self.id
    api_key.update_mutable!(api_user_info, input)
    return api_key
  end

  def update_script_source_config (user, input, request = nil)
    return if (!VendorEndpoint.valid_script_config_parameters?(input) || self.id.nil?)
    raise Api::V1::ApiError.new(:method_not_allowed, "Can't change config of active source") if self.active?

    if (input.key?(:script_config_id))
      code_container = CodeContainer.find(input[:script_config_id])
      ability = Ability.new(user)
      if (!ability.can?(:manage, code_container))
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to code container")
      end
      if (!code_container.reusable?)
        raise Api::V1::ApiError.new(:bad_request, "Cannot reuse that code container")
      end
      vendor_endpoint = input.key?(:vendor_endpoint_name) ? VendorEndpoint.find_by_name(input[:vendor_endpoint_name]) :
        VendorEndpoint.find(input[:vendor_endpoint_id])

      raise Api::V1::ApiError.new(:bad_request, "Invalid vendor endpoint") if vendor_endpoint.nil?

      self.code_container_id = input[:script_config_id]
      self.vendor_endpoint_id = vendor_endpoint.id
    else
      default_script_config = DataSource.default_script_config(self, request)
      if input.key?(:script_config) && !input[:script_config]["config"].nil?
        input[:script_config]["config"] = default_script_config["config"].merge(input[:script_config]["config"])
      else
        input[:script_config] = default_script_config
      end
      input[:script_config].symbolize_keys!
      if !(input[:script_config][:config]).nil?
        email_token = generate_script_email_token
        script_config, vendor_endpoint = VendorEndpoint.get_resource_config(self.data_credentials, input, 'SOURCE',
          input[:script_config][:config], self.id, email_token, "script")
        if email_script_source?(vendor_endpoint)
          script_config["parameters"]["email"] = get_script_source_email(script_config, self.id, request)
        end
        script_config["parameters"]["vendor_endpoint"] = vendor_endpoint.name
        script_config["parameters"]["vendor"] = vendor_endpoint.vendor.name if !vendor_endpoint.vendor.nil?
        code_container = CodeContainer.create_resource_script_config(input,
          script_config, self, user, self.org, CodeContainer::Resource_Types[:source])
        self.code_container_id = code_container.id
        self.vendor_endpoint_id = vendor_endpoint.id
      end
    end

    if !input.key?(:source_config)
      self.source_config = script_source_config(request)
    elsif input.key?(:script_config)
      self.source_config = script_source_config(request).merge(input[:source_config])
    end
    self.save!
  end

  def email_script_source? (vendor_endpoint)
    vendor_endpoint.name.include?("email") || vendor_endpoint.vendor.name.include?("email")
  end

  def get_script_source_email (script_config, id, request)
    env_name = ClusterScriptConfig.config(self, request)[:env_name]
    email_token = script_config["parameters"]["email_token"]
    email = "data-in+" + email_token + "+" + id.to_s
    email += (env_name == "production" ? "" : "+#{env_name}")
    return email + "@nexla.com"
  end

  def script_source_config (request)
    cluster_config = ClusterScriptConfig.config(self, request)
    source_config = {}

    cluster_config.each do |key, value|
      case key
      when :prod_ui_host, :path_prefix, :env_name
        next
      when :path
        source_config[key] = value +
          "/#{cluster_config[:path_prefix]}/#{cluster_config[:env_name]}/#{self.id}"
      else
        source_config[key] = value
      end
    end

    source_config
  end

  def generate_script_email_token
    email_token = self.code_container&.code_config&.dig("parameters", "email_token")
    email_token ||= SecureRandom.hex(4)
    email_token += "-#{self.org.cluster.uid}" if self.org&.cluster&.supports_multi_dataplane?
    email_token
  end

  def script_path_prefix (host_ignored)
    source_config = self.source_config.symbolize_keys
    script_path = ""
    if !source_config[:bucket].nil? && !source_config[:prefix].nil?
      script_path = source_config[:bucket] + "/" + source_config[:prefix]
    elsif !source_config[:path].nil?
      script_path = source_config[:path]
    end
    (script_path.end_with?("/") ? script_path : (script_path + "/"))
  end

  def script_path_bucket
    source_config = self.source_config.symbolize_keys
    script_path = ""
    if !source_config[:bucket].nil?
      script_path = source_config[:bucket] + "/"
    elsif !source_config[:path].nil?
      script_path = (source_config[:path].split("/"))[0] + "/"
    end
    script_path
  end

  def script_path
    # This appears to be unused except in one
    # unit test that checks the statically-configured
    # source path. Could be removed?
    cluster_config = ClusterScriptConfig.config(self)
    return cluster_config[:path]
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

  def activate! (check_tier_limits = true, force = false, run_now: nil)
    return if (self.active? && !force)

    if (self.org.present? && self.org.cluster_migrating?)
      raise Api::V1::ApiError.new(:method_not_allowed,
        "Data source cannot be activated while org's cluster is migrating")
    end

    if (check_tier_limits && !OrgTier.validate_data_source_activate(self))
      raise Api::V1::ApiError.new(:method_not_allowed, "Active data source limit would be exceeded.")
    else
      # Pause and re-activate if already
      # active and force=1 was passed
      self.pause!(false, suppress_notification: true) if self.active?

      #to suppress DataSet notifications in 'after_update' callback
      Notifications::ResourceNotifier.exclusive_for(self) do
        self.transaction do
          self.status = Statuses[:active]
          # Should flow_node update move to after_update handler?
          self.flow_node.status = self.status
          self.flow_node.save!
          self.save!
          self.reload
        end
        if self.active? && (run_now.nil? || run_now.truthy?)
          result = self.send_control_event(:activate)
          Notifications::ResourceNotifier.new(self, :activate).call
          if result.is_a?(Hash) && result[:run_id].present?
            self.transaction do
              self.reload
              self.run_ids.where(run_id: result[:run_id]).first_or_create
              self.last_run_id = result[:run_id]
              self.save!
            end
          end
          result
        end
      end
    end
  end

  def pause! (check_limits = true, suppress_notifications = false)
    return if self.paused?

    if (self.org.present? && self.org.cluster_migrating?)
      raise Api::V1::ApiError.new(:method_not_allowed,
        "Data source cannot be paused while org's cluster is migrating")
    end

    # to suppress DataSet notifications in 'after_update' callback
    Notifications::ResourceNotifier.exclusive_for(self) do
      self.transaction do
        self.status = Statuses[:paused]
        self.flow_node.status = self.status
        self.flow_node.save!
        self.save!
        self.reload
      end
      if self.paused?
        self.send_control_event(:pause)
        Notifications::ResourceNotifier.new(self, :pause).call unless suppress_notifications
      end
    end

    check_account_limit if check_limits
  end

  def check_account_limit
    account_resource = OrgTier.account_resource(self)
    return if (!account_resource.nil? && (account_resource.status == Org::Statuses[:source_data_capped] || account_resource.status == Org::Statuses[:trial_expired]))

    tier_resource = OrgTier.tier_resource(self)
    if !tier_resource.nil? && !self.org.nil?
      limited_sources = DataSource.where(:org_id => self.org.id, :status => DataSource::Statuses[:rate_limited])
      if !limited_sources.blank?
        active_source_count = DataSource.where(:org_id => self.org.id, :status => DataSource::Statuses[:active]).count
        activated_count = active_source_count
        data_source_count_limit = tier_resource.data_source_count_limit.to_i
        if (data_source_count_limit != OrgTier::Unlimited && activated_count < data_source_count_limit)
          limited_sources.each do |source|
            break if (activated_count >= data_source_count_limit)
            source.activate!(false)
            activated_count = (activated_count + 1)
          end
        end
      end
    end
  end

  def source_records_count_capped?
    return unless org&.org_tier

    self.org.status == Org::Statuses[:source_data_capped] && self.status == Statuses[:rate_limited]
  end

  def rate_limited!
    self.status = Statuses[:rate_limited]
    save!
    self.send_control_event(:pause)
  end

  def webhook?
    (self.connector_type == DataSource.connector_types[:nexla_rest]) && (self.ingest_method == Ingest_Methods[:api])
  end

  def webhook_url (api_key = nil)
    return nil unless self.webhook?
    EnvironmentUrl.instance.webhook_url(self, api_key)
  end

  def file_upload?
    (self.connector_type == DataSource.connector_types[:file_upload])
  end

  def file_upload_url (api_key = nil)
    return nil unless self.file_upload?
    EnvironmentUrl.instance.file_upload_url(self, api_key)
  end

  def ai_web_server?
    self.connector_type == DataSource.connector_types[:ai_web_server]
  end

  def ai_web_server_url
    return nil unless self.ai_web_server?
    EnvironmentUrl.instance.ai_web_server_url(self)
  end

  def api_web_server?
    self.connector_type == DataSource.connector_types[:api_web_server]
  end

  def api_web_server_url
    return nil unless self.api_web_server?
    EnvironmentUrl.instance.api_web_server_url(self)
  end

  def auto_generated
    self.data_sink_id.present?
  end

  delegate :nexset_api_connector_types, to: :ConstantResolver

  def nexset_api_compatible?
    nexset_api_connector_types.include?(self.connector_type)
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

  def nexset_api_config
    return nil if !self.nexset_api_compatible?
    config = Hash.new
    config[:data_source_id] = self.id
    config[:source_type] = self.connector_type
    config[:connector_type] = self.connector_type
    config[:params] = extract_all_params(self.source_config).uniq { |param| param[:param] }

    return config
  end

  def flow_dependent_data_source_data (user, org)
    # FN backwards-compatibility
    if user.nil?
      user = self.owner
      org = self.org
    end
    return self.data_sink_id.nil? ? nil : {
      :id => self.id,
      :owner_id => self.owner.id,
      :org_id => self.org&.id,
      :data_credentials => self.data_credentials_id.nil? ? nil : {
        :id => self.data_credentials.id,
        :owner_id => self.data_credentials.owner_id,
        :org_id => self.data_credentials.org_id
      },
      :data_sink => {
        :id => self.data_sink.id,
        :owner_id => self.data_sink.owner_id,
        :org_id => self.data_sink.org_id
      },
      :name => self.name,
      :description => self.description,
      :status => self.status,
      :source_type => self.connector_type,
      :connector_type => self.connector_type,
      :connection_type => self.connector.connection_type,
      :managed => self.managed,
      :auto_generated => true,
      :vendor => nil,
      :access_roles => self.get_access_roles(user, org),
      :copied_from_id => self.copied_from_id,
      :updated_at => self.updated_at,
      :created_at => self.created_at
    }
  end

  def flow_origin (user = nil, org = nil)
    return self
  end

  def flows (downstream_only = false, user = nil, org = nil, admin_level = :none)
    result = DataFlow.empty_flows

    result[:data_sources] << self.flow_resource_data(result, user, org)
    self.data_sets.each do |data_set|
      data_set.flows(false, user, org, admin_level).each do |res_type, res|
        result[res_type.to_sym] += (res.is_a?(Array) ? res : [res])
      end
    end
    return result
  end

  def connection_type
    self.connector&.connection_type
  end

  def flow_attributes (user, org)
    [
      :data_credentials_id,
      :data_credentials_group_id,
      :data_sink_id,
      :auto_generated,
      :managed,
      :ingestion_mode,
      :adaptive_flow,
      :source_type,
      :connector_type,
      :connection_type,
      :template_config,
      :source_config,
      :vendor
    ].map do |attr|
      case attr
      when :connection_type, :source_type
        [ attr, self.raw_source_type(user) ]
      when :vendor
        [ attr, self.vendor_info ]
      else
        [ attr, self.send(attr) ]
      end
    end
  end

  def destroy
    # If already destroyed, return. Called by FlowNode.destroy as association.
    return self if self.id.nil? || self.class.find_by(id: self.id).nil?

    raise Api::V1::ApiError.new(:method_not_allowed, "Data source must be paused before deletion") if self.active?

    Notifications::ResourceNotifier.exclusive_for(self) do
      self.destroy_dependent_data_sets

      # FN backwards-compatibility
      # We no longer have an ActiveRecord association to projects
      # through projects_data_flows, but we might have entries
      # in that table (during transition to full flow-nodes mode).
      # Delete any entries remaining explicitly.
      ProjectsDataFlow.where(data_source_id: self.id).destroy_all

      super
      Notifications::ResourceNotifier.new(self, :delete).call
    end
  end

  def has_template?
    (!(self.vendor_endpoint_id.nil?) && !self.vendor_endpoint.nil?)
  end

  def script_enabled?
    (self.connector_type == DataSource.connector_types[:script] || script_vendor?)
  end

  def script_vendor?
    (!self.vendor_endpoint.nil? && !self.vendor_endpoint.vendor.nil? && self.vendor_endpoint.vendor.connection_type == DataSource.connector_types[:script])
  end

  def script_data_credentials
    return DataCredentials.find_by_id(Script_Data_Credentials_Id)
  end

  def encrypted_credentials
    self.data_credentials.encrypted_credentials if self.data_credentials.present?
  end

  def remove_origin_data_sink
    return if self.data_sink_id.nil?
    self.data_sink_id = nil
    self.save!
  end

  def searchable_attributes
    vendor_code = self.vendor_code
    vendor_name = self.vendor_display_name
    attrs =  {
      data_source_id: self.id,
      source_type: self.source_type,
      connector_code: vendor_code,
      connector_name: vendor_name,
      source_connector_code: vendor_code,
      source_connector_name: vendor_name,
      project_id: origin_node&.project_id,
      nexset_api_compatible: origin_node&.nexset_api_compatible,
      flow_type: origin_node&.flow_type,
      rag: (origin_node&.flow_type == FlowNode::Flow_Types[:rag])
    }
    self.attributes.merge(attrs)
  end

  def data_sets_ids
    data_sets.pluck(:id)
  end

  def send_control_event (event_type)
    return if flow_type == FlowNode::Flow_Types[:rag]
    result = ControlService.new(self).publish(event_type) if self.control_messages_enabled
    if result.is_a?(Hash) && (result[:status] == :ok) && (event_type == :activate)
      # I'm not happy with this, because it depends on a particular format/nesting
      # in control response that could break and we might not notice until run_ids
      # go missing. But 1) I don't have a better idea at the moment, 2) BE will continue
      # to send run_id's via PUT during activate. So if we miss it here, we should
      # get it there.
      result[:run_id] = result.deep_symbolize_keys.dig(:output, :ctrlResult, :message, :run_id)
    end
    result
  end

  def run_now!
    result = activate!(false, true)

    self.update(run_now_at: Time.now, run_now_status: result[:status])

    result
  end

  def ready!
    self.transaction do
      self.status = Statuses[:active]
      # Should flow_node update move to after_update handler?
      self.flow_node.status = self.status
      self.flow_node.save!
      self.save!
      self.reload
    end
  end

  def node_type
    case self.flow_type
    when FlowNode::Flow_Types[:rag]
      :query_prompt
    when FlowNode::Flow_Types[:api_server]
      if self.flow_node_id == self.origin_node_id
        :api_server
      else
        :api_target
      end
    else
      :data_source
    end
  end

  def api_keys
    case self.flow_type
    when FlowNode::Flow_Types[:rag], FlowNode::Flow_Types[:api_server]
      self.service_keys
    else
      self.data_sources_api_keys
    end
  end

  def adaptive_flow_url
    return nil unless self.adaptive_flow?
    EnvironmentUrl.instance.adaptive_flow_url(self)
  end

  def run_profile
    return unless self.adaptive_flow?
    return if self.service_keys.empty?

    service_key = self.service_keys.first
    service_key.attributes.merge({ url: self.adaptive_flow_url })
  end

  def run_variables
    return [] unless self.nexset_api_compatible? && self.adaptive_flow?

    extract_all_params(self.source_config).uniq { |param| param[:param] }
  end

  protected

  def disable_control_messages
    self.control_messages_enabled = false
  end

  def handle_before_destroy
    self.flow_triggers.destroy_all
    self.data_sink.remove_dependent_data_source if self.data_sink.present?
    self.send_control_event(:delete)
  end

  def handle_after_destroy
    code_container.destroy if code_container && !code_container.reusable?
  end

  def handle_after_create_commit
    self.send_control_event(:create)
  end

  def handle_after_create
    self.flow_node.data_source_id = self.id
    self.flow_node.save!

    if (self.webhook? || self.file_upload?)
      msg = self.webhook? ? "sending data to" : "uploading file to"
      DataSourcesApiKey.create({
        :data_source_id => self.id,
        :name => "Key for Data Source: #{self.id}",
        :description => "API key for #{msg} source #{self.id}"
      })
    elsif self.ai_web_server?
      input = {
        name: "Key for RAG flow: #{self.origin_node_id}",
        description: "API key for sending data to source #{self.id}",
        data_source_id: self.id
      }
      ServiceKey.build_from_input(self.owner, self.org, input)
    elsif self.api_web_server?
      input = {
        name: "Key for API Server flow: #{self.origin_node_id}",
        description: "API key for sending data to source #{self.id}",
        data_source_id: self.id
      }
      ServiceKey.build_from_input(self.owner, self.org, input)
    end

    if self.adaptive_flow? && self.service_keys.empty?
      input = {
        name: "Key for adaptive flow: #{self.origin_node_id}",
        description: "API key for sending data to source #{self.id}",
        data_source_id: self.id
      }
      ServiceKey.build_from_input(self.owner, self.org, input)
    end

    if self.parent_data_set_id.present? && self.flow_type == FlowNode::Flow_Types[:api_server]
      api_user_info = ApiUserInfo.new(self.owner, self.org)

      DataSet.build_from_input(
        api_user_info,
        {
          name: "Result",
          description: "Result for target #{self.id}",
          data_source_id: self.id,
          runtime_config: { node_type: :api_result }
        }
      )

      DataSet.build_from_input(
        api_user_info,
        {
          name: "Error",
          description: "Error for target #{self.id}",
          data_source_id: self.id,
          runtime_config: { node_type: :api_error }
        }
      )
    end
  end

  def handle_before_update
    if self.will_save_change_to_data_sink_id?
      parent_node = nil
      if (self.data_sink.present?)
        if !self.data_sink.flow_node.present?
          raise Api::V1::ApiError.new(:internal_server_error,
            "Missing flow node for parent data sink: #{self.data_sink.id}")
        end
        parent_node = self.data_sink.flow_node
      end
      FlowNode.reset_flow_origin(self, parent_node)
    end
  end

  def handle_after_update
    if (self.saved_change_to_status?)
      self.data_sets.each do |data_set|
        self.active? ? data_set.activate! : data_set.pause!
      end
    end

    save_flow_node = false
    if ((self.saved_change_to_owner_id? || self.saved_change_to_org_id?))
      self.api_keys.each(&:save!)
      self.flow_node.owner = self.owner
      self.flow_node.org = self.org
      save_flow_node = true
    end

    if self.saved_change_to_connector_type?
      self.flow_node.nexset_api_compatible = self.nexset_api_compatible?
      save_flow_node = true
    end

    if self.adaptive_flow? && self.service_keys.empty?
      input = {
        name: "Key for adaptive flow: #{self.origin_node_id}",
        description: "API key for sending data to source #{self.id}",
        data_source_id: self.id
      }
      ServiceKey.build_from_input(self.owner, self.org, input)
    end

    self.flow_node.save! if save_flow_node
  end

  def handle_after_update_commit
    self.send_control_event(:update)
  end

  def destroy_dependent_data_sets
    self.data_sets.each(&:destroy)
  end

  def build_flow_node (project = nil)
    return if self.flow_node.present?
    pn_id = nil
    on_id = nil

    if self.data_sink.present?
      if !self.data_sink.flow_node.present?
        raise Api::V1::ApiError.new(:internal_server_error,
          "Missing flow node for parent data sink: #{self.data_sink.id}")
      end
      pn_id = self.data_sink.flow_node.id
      on_id = self.data_sink.flow_node.origin_node_id
    elsif self.parent_data_set_id.present?
      ability = Ability.new(self.owner)
      parent_data_set = DataSet.find(self.parent_data_set_id)
      raise Api::V1::ApiError.new(:bad_request, "Parent data set only allowed for API server flows") if parent_data_set.flow_type != FlowNode::Flow_Types[:api_server]
      raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to parent data set") unless ability.can?(:read, parent_data_set)

      pn_id = parent_data_set.flow_node.id
      on_id = parent_data_set.flow_node.origin_node_id
    end

    flow_type = FlowNode.default_flow_type
    if (self.source_config.is_a?(Hash) && self.source_config[Pipeline_Type_Key].present?)
      flow_type = FlowNode.validate_flow_type(self.source_config[Pipeline_Type_Key])
      if flow_type.nil?
        msg = "Invalid flow type for data source: #{self.source_config[Pipeline_Type_Key]}"
        raise Api::V1::ApiError.new(:bad_request, msg)
      end
    end

    ingestion_mode = ingestion_mode_from_connector

    flow_node = FlowNode.new({
      owner_id: self.owner_id,
      org_id: self.org_id,
      data_source_id: self.id,
      project: project,
      parent_node_id: pn_id,
      origin_node_id: on_id,
      flow_type: flow_type,
      name: self.flow_name.blank? ?
        self.name : self.flow_name,
      description: self.flow_description.blank? ?
        self.description : self.flow_description,
      status: self.status,
      managed: self.managed,
      nexset_api_compatible: self.nexset_api_compatible?,
      ingestion_mode: ingestion_mode
    })

    flow_node.save!
    if flow_node.origin_node_id.nil?
      # Can we avoid this double-save on origin flow_nodes?
      flow_node.parent_node_id = nil
      flow_node.origin_node_id = flow_node.id
      flow_node.save!
    end

    self.flow_node_id = flow_node.id
    self.origin_node_id = flow_node.origin_node_id
  end

  def ingestion_mode_from_connector
    if connector&.ingestion_mode && !script_enabled?
      connector.ingestion_mode
    else
      :full_ingestion
    end
  end
end
