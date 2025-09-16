class FlowNode < ActiveRecord::Base
  self.primary_key = :id

  include Api::V1::Schema
  include AccessControls::Standard
  include Accessible
  include AuditLog
  include Copy
  include Docs
  include SearchableConcern
  include UpdateAclNotificationConcern
  include FlowLinksConcern

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :origin_node, class_name: "FlowNode", foreign_key: "origin_node_id"
  belongs_to :parent_node, class_name: "FlowNode", foreign_key: "parent_node_id"
  belongs_to :shared_origin_node, class_name: "FlowNode", foreign_key: "shared_origin_node_id"
  belongs_to :data_source, dependent: :destroy
  belongs_to :data_set, dependent: :destroy
  belongs_to :data_sink, dependent: :destroy
  belongs_to :cluster
  belongs_to :project
  belongs_to :copied_from, class_name: "FlowNode", foreign_key: "copied_from_id"

  has_many :child_nodes, class_name: "FlowNode", foreign_key: "parent_node_id"
  has_many :left_links, class_name: "FlowLink", foreign_key: "left_origin_node_id", dependent: :destroy
  has_many :right_links, class_name: "FlowLink", foreign_key: "right_origin_node_id", dependent: :destroy

  scope :origins_only, -> { where('origin_node_id = flow_nodes.id') }
  scope :for_search_index, -> {  origins_only }
  scope :search_ignored, -> { where('origin_node_id <> flow_nodes.id') }

  after_save :handle_after_save

  attr_accessor :in_copy, :_runtime_status

  after_initialize do
    self.in_copy = false
  end

  Flow_Types = API_FLOW_TYPES
  Core_Models = [ DataSet, DataSink, DataSource ]
  Accessible_Models = [ CodeContainer, DataCredentials ] + Core_Models

  Condensed_Select_Fields = [
    :id, :origin_node_id, :parent_node_id, :shared_origin_node_id,
    :owner_id, :org_id, :cluster_id, :status,
    :ingestion_mode, :flow_type,
    :data_source_id, :data_set_id, :data_sink_id
  ].freeze

  enum ingestion_mode: API_INGESTION_MODES
  enum flow_type: Flow_Types

  scope :condensed_origins, -> { where("id = origin_node_id").select( Condensed_Select_Fields ) }

  def self.flow_types_enum
    "ENUM(" + Flow_Types.values.map{|v| "'#{v}'"}.join(",") + ")"
  end

  def self.default_flow_type
    Flow_Types[:streaming]
  end

  def self.validate_flow_type (flow_type)
    # There are two keys in use by backend for
    # "pipeline.type" corresponding to in_memory:
    # "in_memory" and "in.memory". We convert the
    # dot form to the underscore form.
    return nil if !flow_type.is_a?(String)

    # See NEX-12980. After deployment of API-3.9, it was discovered
    # that control service crashes when it encounters the 'elt' flow
    # type. There's no UI deployed to create such flows in production,
    # but closing the hole here in case someone tries to create one via
    # direct API calls.
    ft_key = flow_type.downcase.gsub(".", "_").to_sym
    ENV.fetch("API_ENABLE_ELT_FLOWS", true).truthy? ? Flow_Types[ft_key] : Flow_Types.except(:elt)[ft_key]
  end

  def self.build_flow_from_data_source (data_source)
    if !data_source.is_a?(DataSource)
      raise Api::V1::ApiError.new(:internal_server_error,
        "Invalid resource for data source node: #{data_source.class.name}")
    end

    return data_source.flow_node if data_source.flow_node.present?

    begin
      [ DataSource, DataSet, DataSink ].each { |m| PaperTrail.request.disable_model(m) }

      pdf = ProjectsDataFlow.where(data_source_id: data_source.id).first
      project = pdf.present? ? pdf.project : nil

      DataSource.transaction do
        updated_at = data_source.updated_at
        data_source.send(:disable_control_messages)
        data_source.send(:build_flow_node, project)
        data_source.save(validate: false)
        data_source.update_column(:updated_at, updated_at)
      end

      FlowNodesAccessControl.transfer_from_data_flows(:data_source_id, data_source)
      FlowNodesDocContainer.transfer_from_data_flows(:data_source_id, data_source)

      data_source.data_sets.each do |ds|
        FlowNode.build_flow_from_data_set(ds)
      end
    ensure
      [ DataSource, DataSet, DataSink ].each { |m| PaperTrail.request.enable_model(m) }
    end

    return data_source.flow_node
  end

  def self.build_flow_from_data_set (data_set)
    if !data_set.is_a?(DataSet)
      raise Api::V1::ApiError.new(:internal_server_error,
        "Invalid resource for data set node: #{data_set.class.name}")
    end

    return data_set.flow_node if data_set.flow_node.present?

    pdf = ProjectsDataFlow.where(data_set_id: data_set.id).first
    project = pdf.present? ? pdf.project : nil

    DataSet.transaction do
      updated_at = data_set.updated_at
      data_set.send(:disable_control_messages)
      data_set.send(:build_flow_node, project)
      data_set.save(validate: false)
      data_set.update_column(:updated_at, updated_at)
    end

    FlowNodesAccessControl.transfer_from_data_flows(:data_set_id, data_set)
    FlowNodesDocContainer.transfer_from_data_flows(:data_set_id, data_set)

    data_set.child_data_sets.each do |ds|
      FlowNode.build_flow_from_data_set(ds)
    end

    data_set.data_sinks.each do |ds|
      FlowNode.build_flow_from_data_sink(ds)
    end

    return data_set.flow_node
  end

  def self.build_flow_from_data_sink (data_sink)
    if !data_sink.is_a?(DataSink)
      raise Api::V1::ApiError.new(:internal_server_error,
        "Invalid resource for data sink node: #{data_sink.class.name}")
    end

    return data_sink.flow_node if data_sink.flow_node.present?

    # When a data_flow is added to a project by the data_sink_id,
    # the api translates that into the origin data_source_id. So,
    # no need to include project here.
    DataSink.transaction do
      updated_at = data_sink.updated_at
      data_sink.send(:disable_control_messages)
      data_sink.send(:build_flow_node)
      data_sink.save!
      data_sink.update_column(:updated_at, updated_at)
    end

    FlowNodesAccessControl.transfer_from_data_flows(:data_sink_id, data_sink)
    FlowNodesDocContainer.transfer_from_data_flows(:data_sink_id, data_sink)

    if data_sink.data_source.present?
      return FlowNode.build_flow_from_data_source(data_sink.data_source)
    else
      return data_sink.flow_node
    end
  end

  def self.reset_flow_origin (res, parent_node, save_resource = false)
    return if res.flow_node.nil?

    if res.is_a?(DataSource)
      pn_id = nil
      on_id = nil

      if (parent_node.present?)
        pn_id = parent_node.id
        on_id = parent_node.origin_node_id
      else
        on_id = res.flow_node.id
      end

      raise Api::V1::ApiError.new(:internal_server_error,
        "Could not reset flow origin: data source: #{res.id}") if on_id.nil?

      res.flow_node.parent_node_id = pn_id
      res.flow_node.origin_node_id = on_id
      res.flow_node.save!
      res.origin_node_id = res.flow_node.origin_node_id
      res.save! if save_resource

      res.data_sets.each do |ds|
        FlowNode.reset_flow_origin(ds, res.flow_node, true)
      end
    elsif res.is_a?(DataSet)
      pn_id = nil
      on_id = nil
      sn_id = nil

      if parent_node.present?
        pn_id = parent_node.id
        if res.from_shared?(parent_node)
          on_id = res.flow_node.id
          sn_id = parent_node.origin_node_id
        else
          on_id = parent_node.origin_node_id
        end
      else
        on_id = res.flow_node.id
      end

      res.flow_node.parent_node_id = pn_id
      res.flow_node.origin_node_id = on_id
      res.flow_node.shared_origin_node_id = sn_id
      res.flow_node.save!

      res.origin_node_id = res.flow_node.origin_node_id
      res.save! if save_resource

      res.child_data_sets.each do |ds|
        FlowNode.reset_flow_origin(ds, res.flow_node, true)
      end
      res.data_sinks.each do |ds|
        FlowNode.reset_flow_origin(ds, res.flow_node, true)
      end
    elsif res.is_a?(DataSink)
      pn_id = nil
      on_id = nil
      if (parent_node.present?)
        pn_id = parent_node.id
        on_id = parent_node.origin_node_id
      else
        on_id = res.flow_node.id
      end
      res.flow_node.parent_node_id = pn_id
      res.flow_node.origin_node_id = on_id
      res.flow_node.save!
      res.origin_node_id = res.flow_node.origin_node_id
      res.save! if save_resource

      if res.data_source.present?
        FlowNode.reset_flow_origin(res.data_source, res.flow_node, true)
      end
    else
      raise Api::V1::ApiError.new(:internal_server_error,
        "Invalid resource in reset flow origin: #{res.class.name}, #{res.id}")
    end
  end

  def self.accessible_origin_nodes (accessor, access_role, org, scope = nil)
    origin_nodes = FlowNode.accessible(accessor, access_role, org, scope)
      .where("id = origin_node_id")

    return (access_role == :owner) ? origin_nodes :
      origin_nodes.union(Project.accessible_origin_nodes(accessor, access_role, org, scope))
  end

  def self.accessible_resources (model, accessor, access_role = :owner, org = nil)
    resources = model.none
    if !Accessible_Models.include?(model)
      return resources
    end

    if !FlowNode.access_roles.include?(access_role) && (access_role != :all)
      return resources
    end

    fids = FlowNode.accessible_origin_nodes(accessor, access_role, org).pluck(:id)

    if [DataSet, DataSink, DataSource].include?(model)
      resources = resources.union(model.where(origin_node_id: fids))
    elsif (model == DataCredentials)
      ids = DataSource.where(origin_node_id: fids).pluck(:data_credentials_id)
      ids += DataSink.where(origin_node_id: fids).pluck(:data_credentials_id)
      resources = resources.union(model.where(id: ids.compact.uniq))
    elsif (model == CodeContainer)
      ids = DataSet.where(origin_node_id: fids).pluck(:code_container_id)
      resources = resources.union(model.where(id: ids.compact.uniq))
    end

    return resources
  end

  def self.empty_flow
    result = {
      :flow => [],
      :code_containers => [],
      :data_sources => [],
      :data_sets => [],
      :data_sinks => [],
      :data_credentials => [],
      :data_credentials_groups => [],
      :dependent_flow_nodes => [],
      :origin_data_sink_flow_nodes => [],
      :shared_data_sets => [],
      :orgs => [],
      :users => [],
      :projects => []
    }
    result
  end

  def self.origin_nodes (flow_node_ids)
    FlowNode.where(id: flow_node_ids)
      .map(&:origin_node)
      .compact
      .uniq
  end

  def self.origin_node (flow_node_id)
    fn = FlowNode.find(flow_node_id)
    raise Api::V1::ApiError.new(:not_found) if (fn.nil? || fn.origin_node.nil?)
    fn.origin_node
  end

  def same_origin? (flow_node)
    (self.origin_node_id == flow_node.origin_node_id)
  end

  def is_origin?
    (self.id == self.origin_node_id)
  end

  def from_shared?
    self.shared_origin_node_id.present?
  end

  def public?
    false
  end

  def resource_list_key
    self.resource_key.to_s.gsub("_id", "").pluralize.to_sym
  end

  def resource_key
    return :data_source_id if self.data_source_id.present?
    return :data_set_id if self.data_set_id.present?
    return :data_sink_id
  end

  def resource
    return self.data_source if self.data_source.present?
    return self.data_set if self.data_set.present?
    self.data_sink
  end

  def runtime_status
    return self._runtime_status if self._runtime_status.present?

    resource = self.resource
    return API_RUNTIME_STATUSES[:idle] if resource.blank?
    return resource.runtime_status unless self.is_origin?

    processing = API_RUNTIME_STATUSES[:processing]

    self._runtime_status = case
      when DataSource.where(origin_node_id: self.id, runtime_status: processing).exists? then processing
      when DataSet.where(origin_node_id: self.id, runtime_status: processing).exists? then processing
      when DataSink.where(origin_node_id: self.id, runtime_status: processing).exists? then processing
      else API_RUNTIME_STATUSES[:idle]
    end
  end

  delegate :node_type, to: :resource, allow_nil: true

  def last_run_id
    data_source&.last_run_id
  end

  def resources (api_user_info = nil)
    api_user_info ||= ApiUserInfo.new(self.owner, self.org)
    u = api_user_info.user
    u.org = o = api_user_info.org
    r = Hash.new

    origin_ids = [ self.origin_node_id ]
    if (u.super_user? || api_user_info.org_admin?)
      origin_ids += FlowNode.where(shared_origin_node_id: self.origin_node_id, org_id: self.org_id)
                            .pluck(:origin_node_id).uniq
    end

    r[:data_sources] = DataSource.where(origin_node_id: origin_ids).jit_preload
    r[:data_sets] = DataSet.where(origin_node_id: origin_ids).jit_preload
    r[:data_sinks] = DataSink.where(origin_node_id: origin_ids).jit_preload

    ids = (r[:data_sources].map(&:data_credentials_id) +
      r[:data_sinks].map(&:data_credentials_id)).compact.uniq
    r[:data_credentials] = DataCredentials.where(id: ids).jit_preload

    ids = r[:data_sets].map(&:code_container_id).compact.uniq
    r[:code_containers] = CodeContainer.where(id: ids).jit_preload

    r[:shared_data_sets] = []
    if self.origin_node&.shared_origin_node_id.present? && self.origin_node&.parent_node.present?
      r[:shared_data_sets] << self.origin_node.parent_node.data_set
    end

    return r
  end

  def resources_by_model (model, include_group_credentials = false)
    r = model.none
    return r if self.origin_node_id.nil?
    if [DataSource, DataSet, DataSink].include?(model)
      r = model.where(origin_node_id: self.origin_node_id)
    elsif (model == CodeContainer)
      ids = DataSet.where(origin_node_id: self.origin_node_id).pluck(:code_container_id).compact.uniq
      r = CodeContainer.where(id: ids)
    elsif (model == DataCredentials)
      ids = DataSource.where(origin_node_id: self.origin_node_id).pluck(:data_credentials_id)
      ids += DataSink.where(origin_node_id: self.origin_node_id).pluck(:data_credentials_id)

      if include_group_credentials
        group_ids = DataSource.where(origin_node_id: self.origin_node_id).pluck(:data_credentials_group_id)
        group_ids += DataSink.where(origin_node_id: self.origin_node_id).pluck(:data_credentials_group_id)
        group_credentials_ids = DataCredentialsMembership.where(data_credentials_group_id: group_ids).pluck(:data_credentials_id)
        ids += group_credentials_ids
      end

      r = DataCredentials.where(id: ids.compact.uniq)
    end
    return r
  end

  def data_sinks
    DataSink.where(origin_node_id: self.origin_node_id)
  end

  def copy_pre_save (original_flow_node, api_user_info, options)
    # If the original node was an origin node, the new one
    # should be also. It should not point to the old origin.
    # However, we won't know our new id until after the
    # save. See copy_post_save() below.
    self.in_copy = true
    if original_flow_node.is_origin?
      self.origin_node_id = nil
    end

    if original_flow_node.project_id.present?
      # See NEX-16652. Cross-org references to project caused the accessible_by_user()
      # library method to include copied flow_nodes that were not in the caller's org.
      self.project_id = nil if (original_flow_node.project&.org_id != self.org_id)
    end
  end

  def copy_post_save (original_flow_node, api_user_info, options)
    if self.origin_node_id.nil?
      self.origin_node_id = self.id
      self.save!
    end
  end

  def flow (api_user_info = nil, child_node_h = nil, show_triggers = false)
    if (child_node_h.present?)
      fn = self
    else
      if api_user_info.nil?
        self.owner.org = self.org
        api_user_info = ApiUserInfo.new(self.owner, self.org)
      end
      fn = self.origin_node
      child_node_h = fn.preload_flow_nodes
    end

    f = { node: fn, children: Array.new }
    if show_triggers
      f[:triggering_flows] = FlowTrigger.where(triggered_origin_node_id: fn.id).preload(:triggering_flow_node)
    end

    if child_node_h[fn.id].present?
      child_node_h[fn.id].each do |cn|
        next if !fn.can_traverse?(api_user_info, cn)
        f[:children] << cn.flow(api_user_info, child_node_h, false)
      end
    end

    return f
  end

  def id_graph
    [ self.id, self.child_nodes.map(&:id_graph) ]
  end

  def update_mutable! (api_user_info, input, request)
    return if (input.blank? || api_user_info.nil?)

    if (self.is_origin?)
      self.project_id = input[:project_id] if input.key?(:project_id)
    end

    self.save!
  end

  def flow_update_mutable! (api_user_info, input, request)
    return if (input.blank? || api_user_info.nil?)
    if !self.is_origin?
      self.origin_node.flow_update_mutable!(api_user_info, input, request)
      return
    end

    if input.key?(:project_id)
      pid = input[:project_id]
      if !pid.nil?
        project = Project.find(pid)
        if !Ability.new(api_user_info.user).can?(:manage, project)
          raise Api::V1::ApiError.new(:forbidden,
            "Caller cannot manage input project: #{pid}")
        end
      end
      self.project_id = pid
    end

    save_resource = false
    if input.key?(:name)
      self.name = input[:name]
      if self.resource.respond_to?(:flow_name)
        self.resource.flow_name = self.name
        save_resource = true
      end
    end

    if input.key?(:description)
      self.description = input[:description]
      if self.resource.respond_to?(:flow_description)
        self.resource.flow_description = self.description
        save_resource = true
      end
    end

    if input[:flow_type] == Flow_Types[:replication]
      data_sets = self.resources_by_model(DataSet)
      if data_sets.any? { |data_set| data_set.parent_data_set_id.present? }
        raise Api::V1::ApiError.new(:bad_request,
                                    "Flow type cannot be changed to replication because flow has chained nexsets")
      end
    end

    new_flow_type = FlowNode.validate_flow_type(input[:flow_type])
    if new_flow_type
      self.flow_type = new_flow_type
      if self.data_source
        cfg = self.data_source.source_config
        cfg.delete(DataSource::Pipeline_Type_Key)
        cfg[DataSource::Pipeline_Type_Key] = new_flow_type if (new_flow_type != FlowNode.default_flow_type)
        self.data_source.source_config = cfg
        save_resource = true
      end
    end

    self.resource.save! if save_resource

    if (self.owner != api_user_info.input_owner) || (self.org != api_user_info.input_org) ||
      (input[:owner_id].present? && (input[:include_code_containers].truthy? || input[:include_data_credentials].truthy?))
      # if code_containers or data_credentials are included, run chown even if owner is the same
      # if not include - don't, nothing will be changed anyway.
      self.flow_chown!(api_user_info, input)
    end

    self.save!
  end

  def flow_activate! (opts = { :all => false })
    fn = opts[:all].truthy? ? self.origin_node : self
    fn.flow_activate_traverse(true, run_now: opts[:run_now])
  end

  def flow_pause! (opts = { :all => false })
    fn = opts[:all].truthy? ? self.origin_node : self
    fn.flow_activate_traverse(false)
  end

  def flow_copy (api_user_info, options = {}, fn = nil, pfn = nil)
    # Approach: traverse the original flow and copy the node
    # resources at each level. Connect each new resource's
    # flow_node to its parent and the origin node as we go.
    # Bottom out when we have no child nodes or the child
    # nodes have a different origin_node_id (sharing case).

    is_origin = fn.nil?
    fn = self.origin_node if is_origin

    if (pfn.present? && fn.resource.is_a?(DataSource) &&
      !options[:copy_dependent_data_flows])
      pfn.unlink_dependent_source
      return nil
    end

    # DataSource, DataSet and DataSink have copy_pre_save()
    # callbacks which handle creating a copy of the associated
    # flow_node. We finish connecting the new flow node below.
    ds = fn.resource.copy(api_user_info, options)

    if is_origin
      ds.flow_node.origin_node_id = ds.flow_node.id
      ds.flow_node.shared_origin_node_id = cfn.shared_origin_node_id if pfn.present?
    else
      ds.flow_node.origin_node_id = pfn.origin_node_id
    end

    ds.flow_node.parent_node_id = pfn.present? ? pfn.id : nil
    ds.flow_node.save!

    # Note, we want to skip callbacks here. This should
    # not generate a new audit log entry, etc.
    ds.update_columns(origin_node_id: ds.flow_node.origin_node_id)
    ds.flow_node.relink_resource(pfn)

    fn.child_nodes.each do |cfn|
      next if !cfn.same_origin?(fn)
      cfn.flow_copy(api_user_info, options, cfn, ds.flow_node)
    end

    if ds.is_a?(DataSet) && ds.splitter?
      ds.reassign_splitter_children!
    end

    return ds.flow_node
  end

  def flow_chown! (api_user_info, opts = {})
    # Note, flow_chown does not support changing ownership
    # of sub-flows. It's the whole flow or no flow.
    res = Hash.new
    ab = Ability.new(api_user_info.input_owner)
    self.origin_node.flow_chown_preflight(api_user_info, opts, ab, res)

    if res.any? { |k, v| !v.empty? }
      res[:message] = "New owner does not have access to one or more required resources"
      raise Api::V1::ApiError.new(:forbidden, res)
    end

    self.origin_node.flow_chown_traverse(api_user_info, opts)
  end

  def flow_chown_preflight (api_user_info, opts, ab, res)
    if self.project.present? && !ab.can?(:manage, self.project)
      raise Api::V1::ApiError.new(:forbidden,
        "New owner does not have management access to flow project")
    end
    self.resource.chown_preflight(api_user_info, opts, ab, res)
    self.child_nodes.each do |cn|
      next if !cn.same_origin?(self)
      cn.flow_chown_preflight(api_user_info, opts, ab, res)
    end
  end

  def flow_chown_traverse (api_user_info, opts)
    self.resource.chown!(api_user_info, opts)
    self.child_nodes.each do |cn|
      next if !cn.same_origin?(self)
      cn.flow_chown_traverse(api_user_info, opts)
    end
  end

  def flow_destroy (opts = { :all => false })
    fn = opts[:all].truthy? ? self.origin_node : self

    sharers = fn.sharers_with_flows
    if !sharers[:sharers].empty?
      sharers[:message] = "Flow cannot be deleted while shared nexsets have downstream flows"
      raise Api::V1::ApiError.new(:method_not_allowed, sharers)
    end

    rag_flow_ids = linked_rag_flow_ids
    if rag_flow_ids.present?
      raise Api::V1::ApiError.new(:method_not_allowed, { linked_rag_flow_ids: rag_flow_ids, message: "Flow cannot be deleted while linked to a RAG flow" })
    end

    res_a = {
      :data_sources => Array.new,
      :data_sets => Array.new,
      :data_sinks => Array.new
    }
    res_a = fn.flow_gather_active(res_a)
    active_count = 0
    res_a.each {|k, v| active_count += v.count }

    if (active_count > 0)
      res_a[:message] = "Active flow resources must be paused before flow deletion!"
      raise Api::V1::ApiError.new(:method_not_allowed, res_a)
    end

    fn.flow_destroy_traverse
  end

  def active?
    self.status == 'ACTIVE'
  end

  def flow_gather_active (res_a)
    if self.resource.active?
      res_a[self.resource_list_key] << self.resource.id
    end
    self.child_nodes.each do |cn|
      next if !cn.same_origin?(self)
      cn.flow_gather_active(res_a)
    end
    return res_a
  end

  def flow_destroy_traverse
    # Move to protected section
    # We delete flows from the bottom up, so delete the
    # current node after deleting its chilren. Also, we
    # rely on the :dependent_destroy setting on resource
    # associations to delete the data_source, data_set
    # or data_sink.

    self.child_nodes.each do |cn|
      next if !cn.same_origin?(self)
      cn.flow_destroy_traverse
    end

    if self.resource.respond_to?(:force_delete)
      self.resource.force_delete = true
    end
    self.resource.destroy # this destroys resource BEFORE node is destroyed, so control payload can be formed correctly.
    self.destroy
  end

  def sharers_with_flows
    nodes_from_shared = FlowNode.where(shared_origin_node_id: self.origin_node_id)
    return { sharers: [] } if nodes_from_shared.empty?
    return {
      sharers: nodes_from_shared.map do |fn|
        if (fn.data_set.present?)
          {
            email: fn.data_set.owner.email,
            org: fn.data_set.org.name,
            org_id: fn.data_set.org.id
          }
        else
          nil
        end
      end
      .compact
      .uniq
    }
  end

  def should_index?
    self.is_origin?
  end

  def can_traverse? (api_user_info, child_node)
    # The happy path, in the same flow.
    return true if (child_node.origin_node_id == self.origin_node_id)

    # The ONLY valid flow boundary (for now) is one created
    # by deriving a data set from a shared data set. The derived
    # data set contains a reference to the origin of the shared
    # set. If that is not present, we can't traverse.
    return false if (child_node.shared_origin_node_id != self.origin_node_id)

    # The remaining cases are flows derived from shared data sets.
    # Such boundaries may or may not span Org boundaries, which
    # affects traversal permission.

    # Check for sharer. Only exact "sharer" role.
    if self.data_set
      return true if self.data_set.get_access_roles(api_user_info.user, api_user_info.org, false) == [:sharer]
    end

    # Inter-org boundary (including no-Org) can only
    # traversed by super user, even if the node owner
    # is the same in the different contexts.
    return api_user_info.user.super_user? if (child_node.org != org)

    # We're not crossing an Org boundary. If child node
    # is owned by the traverser, it's ok (though this kind
    # of flow should probably not be constructed).
    return true if (child_node.owner_id == api_user_info.user.id)

    # Intra-org sharing. Only org admins can traverse this boundary.
    if api_user_info.org.present?
      return api_user_info.org_admin?
    end

    # You're out of luck.
    return false
  end

  def audit_log_traverse(date_interval = DateInterval.new(:current_and_previous_q), cnd = {}, result = [])
    result.concat(self.audit_log(date_interval, cnd))

    self.child_nodes.each do |child|
      next unless child.same_origin?(self)
      child.audit_log_traverse(date_interval, cnd, result)
    end

    result
  end

  def source_records_count_capped?
    self.data_source&.source_records_count_capped?
  end

  def triggered_flows(user)
    sink = DataSink.where(origin_node_id: self.id).last
    return [] unless sink

    node_ids = FlowTrigger.where(triggering_flow_node_id: sink.flow_node_id).pluck(:triggered_origin_node_id)
    user.flow_nodes(self.org, access_role: :all, selected_ids: node_ids).map(&:origin_node).uniq
  end

  def triggering_flows(user)
    node_ids = FlowTrigger.where(triggered_origin_node_id: self.origin_node_id).pluck(:triggering_flow_node_id)
    user.flow_nodes(self.org, access_role: :all, selected_ids: node_ids).map(&:origin_node).uniq
  end

  def insert_flow_node (api_user_info, input, params, request)
    ability = Ability.new(api_user_info.user)
    node = nil
    FlowNode.transaction do
      if input.key?(:data_set_id)
        resource = DataSet.find(input[:data_set_id])
        unless ability.can?(:manage, resource)
          raise Api::V1::ApiError.new(:forbidden)
        end
        unless resource.flow_node&.child_nodes&.empty?
          raise Api::V1::ApiError.new(:bad_request, "Cannot insert a data_set node that has child data_sets or data_sinks")
        end
      elsif input.key?(:data_set)
        resource = create_node_resource(api_user_info, input, params, request)
      end

      if resource.blank?
        raise Api::V1::ApiError.new(:bad_request, "Invalid input for flow node resource creation. Supported keys: data_set, data_set_id")
      end

      verify_resource_insertion!
      node = insert_node!(resource)
    end
    node
  end

  def remove_flow_node(delete_resource: false)
    if self.is_origin? || self.data_source_id.present?
      raise Api::V1::ApiError.new(:bad_request, "Cannot remove origin node")
    end

    unless self.data_set.present?
      raise Api::V1::ApiError.new(:bad_request, "Cannot remove non-data_set nodes from flows")
    end

    unless self.parent_node.present?
      raise Api::V1::ApiError.new(:bad_request, "Cannot remove node with no parent node")
    end

    unless self.parent_node.data_set.present?
      raise Api::V1::ApiError.new(:bad_request, "Cannot remove node whose parent node is not a data_set")
    end

    parent_resource = self.parent_node.resource

    FlowNode.transaction do
      self.child_nodes.each do |fn|
        if fn.data_set.present?
          fn.data_set.update(parent_data_set_id: parent_resource.id)
        elsif fn.data_sink.present?
          fn.data_sink.update(data_set_id: parent_resource.id)
        end
        fn.parent_node_id = self.parent_node_id
      end

      if delete_resource
        self.destroy
      else
        # Resource becomes stand-alone flow
        self.resource.parent_data_set_id = nil
        self.resource.origin_node_id = self.resource.flow_node_id
        self.resource.save!
        self.origin_node_id = self.id
        self.save!
      end
    end
  end

  def run_profile_activate!(input, api_user_info)
    raise Api::V1::ApiError.new(:bad_request, "Flow node is not a flow origin") unless self.is_origin?
    raise Api::V1::ApiError.new(:bad_request, "External triggers are only available for DirectFlow") unless self.flow_type == Flow_Types[:in_memory]
    raise Api::V1::ApiError.new(:bad_request, "Run profile input must be an array") unless input.is_a?(Array)

    data_source = self.data_source
    raise Api::V1::ApiError.new(:bad_request, "Invalid data source resource") if data_source.blank?
    raise Api::V1::ApiError.new(:method_not_allowed, "Data source is not active") unless data_source.active?
    raise Api::V1::ApiError.new(:method_not_allowed, "Incorrect ingestion scheduling mode") unless data_source.adaptive_flow?

    data_credentials_ids, data_sources_ids, data_sinks_ids, request_body = fetch_run_profile_resource_ids(input, self.id, data_source.id, self.org_id)
    validate_run_profile_resources!(data_credentials_ids, data_sources_ids, data_sinks_ids, api_user_info)

    ListingService.new.run_profiles_batch(data_source, request_body.to_json)
  end

  def searchable_attributes
    self.attributes.merge(is_origin: is_origin?)
  end

  protected

  def create_node_resource(api_user_info, input, params, request={})
    return if input.blank?

    if input.key?(:data_set)
      resource_input = input[:data_set].merge(parent_data_set_id: self.data_set_id)
      DataSet.build_from_input(api_user_info, resource_input, params[:use_source_owner], params[:detected])
    end
  end

  def verify_resource_insertion!
    if self.resource.is_a?(DataSource) && self.resource.data_sets.exists?
      raise Api::V1::ApiError.new(:bad_request, "Cannot insert nodes between data source and detected nexsets")
    end

    if self.resource.is_a?(DataSink)
      raise Api::V1::ApiError.new(:bad_request, "Cannot insert nodes after data sink")
    end
  end

  def insert_node!(resource)
    existing_children = FlowNode.where(parent_node_id: resource.flow_node_id)

    reparent_children = self.child_nodes.where.not(id: resource.flow_node_id).to_a

    resource.flow_node.parent_node_id = self.id
    resource.flow_node.origin_node_id = self.origin_node_id
    resource.flow_node.save!

    if resource.is_a?(DataSet) && self.data_set_id
      resource.parent_data_set_id = self.data_set_id
    end

    if resource.is_a?(DataSink) && self.data_set_id
      resource.data_set_id = self.data_set_id
    end

    if existing_children.size > 0
      FlowNode.where(parent_node_id: resource.flow_node_id)
              .update(origin_node_id: self.origin_node_id)
    end

    resource.save! if resource.changed?

    reparent_children.each do |child_node|
      child_node.parent_node_id = resource.flow_node_id
      if child_node.resource.is_a?(DataSet)
        child_node.resource.update(parent_data_set_id: resource.id)
      elsif child_node.resource.is_a?(DataSink)
        child_node.resource.update(data_set_id: resource.id)
      end
    end

    resource.flow_node
  end

  def handle_after_save
    return if !self.resource.present?

    # If this is a new flow_node being copied from an
    # existing one, we do not check for owner/org changes
    # here because the new copied resource has not be set yet.
    return if self.in_copy

    if (self.saved_change_to_owner_id? || self.saved_change_to_org_id?)
      need_save = false
      if (self.resource.owner != self.owner)
        self.resource.owner = self.owner
        need_save = true
      end
      if (self.resource.org != self.org)
        self.resource.org = self.org
        need_save = true
      end
      self.resource.save! if need_save
    end
  end

  def flow_activate_traverse (activate, run_now: nil)
    if activate
      self.resource.activate!
      case self.resource
      when DataSource
        self.resource.activate!(run_now: run_now)
      else
        self.resource.activate!
      end
    else
      self.resource.pause!
    end
    self.child_nodes.each do |cn|
      next if !cn.same_origin?(self)
      cn.flow_activate_traverse(activate)
      cn.flow_activate_traverse(activate, run_now: run_now)
    end
  end

  def preload_flow_nodes
    origin_ids = [ self.origin_node_id ]
    origin_ids += FlowNode.where(shared_origin_node_id: self.origin_node.id)
      .pluck(:origin_node_id).uniq
    flow_nodes = FlowNode.where(origin_node_id: origin_ids).jit_preload
    child_node_h = Hash.new
    flow_nodes.each do |fn|
      next if fn.parent_node_id.nil?
      child_node_h[fn.parent_node_id] ||= Array.new
      child_node_h[fn.parent_node_id] << fn
    end
    return child_node_h
  end

  def relink_resource (parent_node)
    res = self.resource
    if res.is_a?(DataSource)
      if parent_node.present?
        self.relink_dependent_source(parent_node)
      else
        res.update_columns(data_sink_id: nil)
      end
    elsif res.is_a?(DataSet)
      self.relink_data_set(parent_node)
    elsif res.is_a?(DataSink)
      self.relink_data_sink(parent_node)
    else
      raise Api::V1::ApiError.new(:internal_server_error,
        "Invalid flow resource: #{res.class.name}, #{res.id}")
    end
  end

  def relink_data_sink (parent_node)
    res = self.resource

    if !parent_node.present?
      res.update_columns(data_set_id: nil)
      return
    end

    pres = parent_node.resource
    if !pres.is_a?(DataSet)
      raise Api::V1::ApiError.new(:internal_server_error,
        "Invalid flow parent for data ink: #{pres.class.name}, #{pres.id}")
    end

    res.update_columns(data_set_id: pres.id)
  end

  def relink_data_set (parent_node)
    res = self.resource

    if !parent_node.present?
      res.update_columns(data_source_id: nil, parent_data_set_id: nil)
      return
    end

    pres = parent_node.resource
    if pres.is_a?(DataSource)
      res.update_columns(data_source_id: pres.id, parent_data_set_id: nil)
    elsif pres.is_a?(DataSet)
      res.update_columns(data_source_id: nil, parent_data_set_id: pres.id)
    else
      raise Api::V1::ApiError.new(:internal_server_error,
        "Invalid flow parent for data set: #{pres.class.name}, #{pres.id}")
    end
  end

  def unlink_dependent_source
    sink = self.resource
    return if !sink.is_a?(DataSink)

    cfg = sink.sink_config
    cfg.delete(DataSink::Create_Data_Source_Key)
    cfg.delete(DataSink::Ingest_Data_Source_Key)
    sink.update_columns(data_source_id: nil, sink_config: cfg.to_json)
  end

  def relink_dependent_source (parent_node)
    src = self.resource
    return if !src.is_a?(DataSource)
    sink = parent_node.resource

    if (!sink.is_a?(DataSink))
      src.update_columns(data_sink_id: nil)
      return
    end

    src.update_column(:data_sink_id, sink.id)
    cfg = sink.sink_config
    cfg[DataSink::Create_Data_Source_Key] = true
    cfg[DataSink::Ingest_Data_Source_Key] = src.id
    sink.update_columns(data_source_id: src.id, sink_config: cfg.to_json)
  end

  private

  def fetch_run_profile_resource_ids(input, origin_node_id, data_source_id, flow_org_id)
    data_credentials_ids = Set.new
    data_sources_ids = Set.new
    data_sinks_ids = Set.new
    request_body = []

    input.each_with_index do |run, index|
      RunProfileParameters.validate_input_schema(run)

      run['name'] = "Task #{index + 1}" if run['name'].blank?

      run['sources']&.each do |source|
        data_sources_ids << source['id']
        data_credentials_ids << source['data_credentials_id'] if source['data_credentials_id']
      end

      run['sinks']&.each do |sink|
        data_sinks_ids << sink['id']
        data_credentials_ids << sink['data_credentials_id'] if sink['data_credentials_id']
      end

      request_body << {
        flow_id: origin_node_id,
        source_id: data_source_id,
        org_id: flow_org_id,
        parameters: run
      }
    end

    return data_credentials_ids.to_a, data_sources_ids.to_a, data_sinks_ids.to_a, request_body
  end

  def validate_run_profile_resources!(data_credentials_ids, data_sources_ids, data_sinks_ids, api_user_info)
    user = api_user_info.user
    org = api_user_info.org

    accessible_sources_ids = DataSource.accessible_by_user(user, org, { access_role: :collaborator, selected_ids: data_sources_ids })
      .where(origin_node_id: self.id)
      .pluck(:id)
    accessible_sinks_ids = DataSink.accessible_by_user(user, org, { access_role: :collaborator, selected_ids: data_sinks_ids })
      .where(origin_node_id: self.id)
      .pluck(:id)

    if (missing_sources = data_sources_ids - accessible_sources_ids).present?
      raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to sources #{missing_sources}")
    end

    if (missing_sinks = data_sinks_ids - accessible_sinks_ids).present?
      raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to sinks #{missing_sinks}")
    end

    accessible_creds_ids = DataCredentials.accessible_by_user(user, org, { access_role: :collaborator, selected_ids: data_credentials_ids })
      .pluck(:id)
    flow_credentials_ids = self.resources_by_model(DataCredentials, true).pluck(:id)

    missing_credentials = data_credentials_ids - accessible_creds_ids
    missing_credentials += data_credentials_ids - flow_credentials_ids
    missing_credentials.uniq!

    if missing_credentials.present?
      raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to credentials #{missing_credentials}")
    end
  end
end
