class DataFlow
  include ActiveModel::Model
  include Api::V1::Schema
  include FlowBuilder
  include AccessControlsDataFlows
  include UpdateAclNotificationConcern

  attr_accessor :data_set_id, :data_set, :data_sets
  attr_accessor :data_source_id, :data_source, :data_sources
  attr_accessor :data_sink_id, :data_sink, :data_sinks
  attr_accessor :user, :org, :params
  attr_accessor :enabling_acls
  attr_accessor :resource_key
  attr_accessor :flows_access_roles

  Resource_Keys = [ :data_source_id, :data_set_id, :data_sink_id ]

  # These resource keys are used to look up
  # resources from flows that are accessible to
  # a user, team or org based on ACL access to
  # the flow. ONLY these resource types are made
  # accessible by data flow ACL access.
  Accessible_Keys = [
    :code_containers,
    :data_sources,
    :data_sets,
    :data_sinks,
    :data_credentials
  ] 

  def self.resource_key_model (resource_key)
    resource_key.to_s.gsub("_id", "").camelcase.constantize
  end

  def self.empty_flows
    result = {
      :flows => [],
      :code_containers => [],
      :data_sources => [],
      :data_sets => [],
      :data_sinks => [],
      :data_credentials => [],
      :dependent_data_sources => [],
      :origin_data_sinks => [],
      :shared_data_sets => [],
      :orgs => [],
      :users => [],
      :projects => []
    }
    result 
  end

  def self.empty_cache
    cache = Hash.new
    cache[:data_source] = Hash.new
    cache[:data_sink] = Hash.new
    cache[:data_set] = Hash.new
    cache
  end

  def self.find (params)
    params[:user] = current_user
    params[:org] = current_org

    if (params.key?(:data_flow_id))
      params[:data_set_id] = params[:data_flow_id]
    end

    if !params.key?(:data_source_id) && !params.key?(:data_sink_id) && !params.key?(:data_set_id)
      raise Api::V1::ApiError.new(:not_found)
    end

    return DataFlow.new(params)
  end

  def self.merge_flows (flows)
    merged_flows = DataFlow.empty_flows
    flows.each do |flow|
      merged_flows.keys.each do |key|
        merged_flows[key] = (merged_flows[key] + flow[key]).uniq
      end
    end
    merged_flows[:flows] = DataFlow.merge_common_flow_list(merged_flows[:flows])
    return merged_flows
  end

  def self.merge_common_flow_list (flows_list)
    flows_h = Hash.new
    flows_n = Array.new

    flows_list.each do |f| 
      flows_h[f[:id]] ||= Array.new
      flows_h[f[:id]] << f
    end

    flows_h.each do |id, flows|
      while (flows.size > 1) do
        fm = DataFlow.merge_common_flows(flows.shift, flows.shift)
        flows.unshift(fm) if fm.is_a?(Hash)
      end
      flows_n << flows.first
    end

    return flows_n
  end

  def self.merge_common_flows (f1, f2)
    return f1 if (f1.is_a?(Hash) && !f2.is_a?(Hash))
    return f2 if (f2.is_a?(Hash) && !f1.is_a?(Hash))
    return nil if (!f1.is_a?(Hash) && !f2.is_a?(Hash))
    return nil if (f1[:id] != f2[:id])
    fm = f1.deep_dup
    if (fm[:children] != f2[:children])
      fm[:children] = DataFlow.merge_common_flow_list(
        fm[:children] + f2[:children].deep_dup)
    end
    return fm
  end

  def initialize (params)
    self.data_set_id = params[:data_set_id]
    self.data_sets = params[:data_sets]
    self.data_source_id = params[:data_source_id]
    self.data_sources = params[:data_sources]
    self.data_sink_id = params[:data_sink_id]
    self.data_sinks = params[:data_sinks]
    self.user = params[:user]
    self.org = params[:org]
    self.params = params.deep_dup
    self.init_resource
    self.enabling_acls = nil
    @admin_level = nil
    @flows = nil
    @flows_options = nil
  end

  def self.flows_from_resources (resources, user, org = nil)
    if resources[:data_credential_ids].present?
      resources[:data_source_ids] +=
        DataSource.where(data_credentials_id: resources[:data_credential_ids])
          .map(&:id)

        DataSink.where(data_credentials_id: resources[:data_credential_ids]).each do |d|
          fo = d.flow_origin(user, org)
          if fo.is_a?(DataSource)
            resources[:data_source_ids] << fo.id
          elsif fo.is_a?(DataSet)
            resources[:data_set_ids] << fo.id
          end
       end
    end

    flows_params = {
      :user => user,
      :org => org,
      :data_source_where => { :id => resources[:data_source_ids] },
      :data_set_where => {}
    }

    f1 = DataFlow.new(flows_params).flows_quick

    data_sink_ids = resources[:data_sink_ids] -
      f1[:data_sinks].pluck(:id)

    data_set_ids = resources[:data_set_ids] -
      f1[:data_sets].pluck(:id)

    src_ids = []
    DataSink.where(:id => data_sink_ids).each do |ds|
      fo = ds.flow_origin(user, org)
      if fo.is_a?(DataSource)
        src_ids << fo.id
      elsif fo.is_a?(DataSet)
        data_set_ids << fo.id
      end
    end

    return f1 if (src_ids.empty? && data_set_ids.empty?)
    all_flows = [ f1 ]

    if !src_ids.empty?
      flows_params[:data_source_where] = { :id => src_ids }
      f2 = DataFlow.new(flows_params).flows_quick
      data_set_ids = data_set_ids -
        f2[:data_sets].pluck(:id)
      all_flows << f2
    end

    return DataFlow.merge_flows(all_flows) if data_set_ids.empty?

    src_ids = []
    set_ids = []
    DataSet.where(:id => data_set_ids).each do |ds|
      fo = ds.flow_origin(user, org)
      if fo.is_a?(DataSource)
        src_ids << fo.id
      elsif fo.is_a?(DataSet)
        set_ids << fo.id
      end
    end

    if !src_ids.empty?
      flows_params[:data_source_where] = { :id => src_ids }
      all_flows << DataFlow.new(flows_params).flows_quick
    end

    if !set_ids.empty?
      flows_params[:data_source_where] = {}
      flows_params[:data_set_where] = { :id => set_ids }
      all_flows << DataFlow.new(flows_params).flows_quick
    end

    return DataFlow.merge_flows(all_flows)
  end

  def has_admin_access? (accessor)
    if (self.resource.nil? || self.resource.origin_node.nil?)
      has_access = self._has_admin_access?(accessor)
      return has_access
    end
    self.resource.origin_node.has_admin_access?(accessor)
  end

  def has_collaborator_access? (accessor)
    if (self.resource.nil? || self.resource.origin_node.nil?)
      has_access = self._has_collaborator_access?(accessor)
      return has_access
    end
    self.resource.origin_node.has_collaborator_access?(accessor)
  end

  def origin_flow
    return self if !self.data_source.nil?

    ds = self.resource.flow_origin
    if (ds.is_a?(DataSource))
      res_key = :data_source_id
    elsif (ds.is_a?(DataSet))
      if (self.resource.is_a?(DataSet) && (self.resource.id == ds.id))
        return self
      end
      res_key = :data_set_id
    else
      # This will happen when a DataFlow instance is created
      # with a data sink that is not associated with a data set.
      return nil
    end

    return DataFlow.new({
      res_key => ds.id, :user => self.user, :org => self.org
    })
  end

  def init_resource
    self.data_source = nil
    self.data_sink = nil
    self.data_set = nil

    if (!self.data_source_id.nil?)
      self.data_source = DataSource.find(self.data_source_id)
      self.resource_key = :data_source_id
      @assoc_where = { :data_source_id => self.data_source_id }
    elsif (!self.data_sink_id.nil?)
      self.data_sink = DataSink.find(self.data_sink_id)
      self.resource_key = :data_sink_id
      @assoc_where = { :data_sink_id => self.data_sink_id }
    elsif (!self.data_set_id.nil?)
      self.data_set = DataSet.find(self.data_set_id)
      self.resource_key = :data_set_id
      @assoc_where = { :data_set_id => self.data_set_id }
    end

    origin_node = self.resource&.origin_node
    if origin_node.present?
      self.flows_access_roles = origin_node.get_access_roles(self.user, self.org, false)
    end
  end

  def resource
    return self.data_source if !self.data_source.nil?
    return self.data_sink if !self.data_sink.nil?
    return self.data_set
  end

  def contains_resource? (resource)
    if (!self.resource.nil? && (self.resource.class == resource.class))
      return true if (self.resource.id == resource.id)
    end
    f = self.flows
    resource_sym = resource.class.name.underscore.pluralize.to_sym
    resource_sym = :code_containers if [:transforms, :attribute_transforms].include?(resource_sym)
    return false if f[resource_sym].nil?
    return !f[resource_sym].find { |r| r[:id] == resource.id }.nil?
  end

  def docs
    return DocContainer.none if self.resource.nil?
    ids = FlowNodesDocContainer.where(flow_node_id: self.resource.origin_node_id)
      .pluck(:doc_container_id)
    return DocContainer.where(id: ids)
  end

  def add_docs (dc)
    origin_node_id = self.resource.origin_node_id
    dc = [dc] if dc.is_a?(DocContainer)
    existing_docs = self.docs
    dc.each do |d|
      if (!existing_docs.include?(d))
        FlowNodesDocContainer.create(flow_node_id: origin_node_id, doc_container_id: d.id)
      end
    end
    return self.docs
  end

  def delete_docs (dc)
    origin_node_id = self.resource.origin_node_id
    if (dc == :all)
      FlowNodesDocContainer.where(flow_node_id: origin_node_id).delete_all
    else
      dc = [dc] if dc.is_a?(DocContainer)
      dc.each do |d|
        FlowNodesDocContainer.where(flow_node_id: origin_node_id, doc_container_id: d.id)
          .delete_all
      end
    end
    return self.docs
  end

  def audit_log (date_interval, sort = false)
    f = self.flows

    if (self.resource.present?)
      audit_entries = DataFlow.ac_model.audit_log(date_interval, {
        org_id: self.org.id,
        resource_id: self.resource.id,
        resource_type: self.resource.class.name
      })
    else
      audit_entries = DataFlow.ac_versions_model.none
    end

    [ DataSource, DataSet, DataSink, DataCredentials, CodeContainer ].each do |model|
      res_type = model.name.pluralize.underscore.to_sym
      f[res_type].each do |d|
        res = model.find_by_id(d[:id])
        next if res.nil?
        audit_entries += res.audit_log(date_interval)
      end
    end

    return (sort ? AuditEntry.sort_by_date(audit_entries) : audit_entries)
  end

  def reload
    @admin_level = nil
    @flows_options = { :reload => rand }
    @flows = nil
    self.init_resource
  end

  def admin_level
    self.init_admin_level if (@admin_level.nil?)
    return @admin_level
  end

  def is_admin?
    self.init_admin_level if (@admin_level.nil?)
    return (@admin_level != :none)
  end

  def is_super_user?
    self.init_admin_level if (@admin_level.nil?)
    return (@admin_level == :super)
  end    

  def init_admin_level
    return if !@admin_level.nil?
    @admin_level = :none
    return if (self.user.nil? || self.org.nil?)
    if (self.user.super_user?)
      @admin_level = :super
    elsif (self.org.has_admin_access?(self.user))
      @admin_level = :org
    end
  end

  def cached_flows_valid (flow_options)
    return false if @flows_options.nil?
    options_match = true
    @flows_options.keys.each do |key|
      if (@flows_options[key] != flow_options[key])
        options_match = false
        break
      end
    end
    return options_match
  end

  def flows (downstream_only = false, full_tree = false)
    flows_options = {
      :downstream_only => downstream_only, :full_tree => full_tree
    }

    return @flows if self.cached_flows_valid(flows_options)
    @flows_options = flows_options

    if (!self.enabling_acls.blank?)
      @flows = self.flows_from_enabling_acls(downstream_only, full_tree)
      return @flows
    end

    result = nil
    if (!self.data_sources.nil?)
      result = DataFlow.empty_flows
      self.data_sources.each do |data_source|
        self.data_source_id = data_source.id
        self.init_resource
        data_source.flows(false, self.user, self.org, self.admin_level).each do |res_type, res|
          if (res.is_a?(Hash))
            result[res_type.to_sym] << res
          elsif (res.is_a?(Array))
            result[res_type.to_sym] += res
          end
        end
      end
      if (!self.data_sets.nil?)
        self.data_source_id = self.data_set_id = self.data_sink_id = nil
        self.data_sets.each do |data_set|
          self.data_set_id = data_set.id
          self.init_resource
          data_set.flows(downstream_only, self.user, self.org, self.admin_level).each do |res_type, res|
            if (res.is_a?(Hash))
              result[res_type.to_sym] << res
            elsif (res.is_a?(Array))
              result[res_type.to_sym] += res
            end
          end
        end
      end
    elsif (!self.data_source.nil?)
      result = self.data_source.flows(false, self.user, self.org, self.admin_level)
    elsif (!self.data_sinks.nil?)
      result = DataFlow.empty_flows
      self.data_source_id = self.data_sink_id = self.data_set_id = nil
      self.data_sinks.each do |data_sink|
        self.data_sink_id = data_sink.id
        self.init_resource
        if (full_tree && !data_sink.data_set.nil?)
          data_sink.flow_origin(self.user, self.org).flows(downstream_only, self.user, self.org, self.admin_level).each do |res_type, res|
            if (res.is_a?(Hash))
              result[res_type.to_sym] << res
            elsif (res.is_a?(Array))
              result[res_type.to_sym] += res
            end
          end
        else
          data_sink.flows(downstream_only, self.user, self.org, self.admin_level).each do |res_type, res|
            if (res.is_a?(Hash))
              result[res_type.to_sym] << res
            elsif (res.is_a?(Array))
              result[res_type.to_sym] += res
            end
          end
        end
      end
    elsif (!self.data_sink.nil?)
      if (full_tree && !self.data_sink.data_set.nil?)
        result = self.data_sink.flow_origin(self.user, self.org).flows(downstream_only, self.user, self.org, self.admin_level)
      else
        result = self.data_sink.flows(downstream_only, self.user, self.org, self.admin_level)
      end
    else
      if full_tree
        result = self.data_set.flow_origin(self.user, self.org).flows(downstream_only, self.user, self.org, self.admin_level)
      else
        result = self.data_set.flows(downstream_only, self.user, self.org, self.admin_level)
      end        
    end

    tmp = Hash.new
    result.each do |res_type, res|
      res_type = res_type.to_sym
      if (res_type == :flows)
        tmp[res_type] = (res.is_a?(Array) ? res : [res])
      else
        tmp[res_type] = (res.is_a?(Array) ? res.compact.uniq {|r| r[:id] } : [res])
      end
    end

    if (!self.resource.nil? && !self.project.nil?)
      tmp[:projects] << {
        :id => self.project.id,
        :name => self.project.name,
        :description => self.project.description
      }
      tmp[:projects] = tmp[:projects].uniq
    end

    apply_flow_tags(tmp)
    apply_flow_access_roles(tmp)
    apply_flow_docs(tmp)
  
    @flows = tmp
    return @flows
  end

  def flows_from_enabling_acls (downstream_only = false, full_tree = false)
    # If enabling_acls is non-empty array, one of the has_xyz_access?() 
    # methods was called and identified a one or more flow views enabled
    # by an ACL rule associated with a resource different from the instiating
    # resource. For example, a data_flow ACL associated with a data_sink 
    # can enable a view from a data source which was used to instatiate the
    # current DataFlow object.
    all_flows = Array.new
    all_flows += self.enabling_acls.map { |acl|
      acl.data_flow(self.user, self.org).flows(downstream_only, full_tree)
    }
    
    return DataFlow.merge_flows(all_flows)
  end

  def provisioning_flow (logger)
    if (!self.user.infrastructure_user? && !self.user.super_user?)
      raise Api::V1::ApiError.new(:forbidden)
    end

    if self.data_sink.nil?
      logger.error("data_sink missing: #{self.data_sink_id}")
      return nil
    end
    
    if self.data_sink.data_set.nil?
      logger.error("Data sink does not have a data_set: #{self.data_sink.id}")
      return nil
    end

    flow = self.data_sink.flows(false, self.user, self.org, true)
    flow[:provisioning_flow] = true
    flow.delete(:orgs)
    flow.delete(:users)

    flow_node = flow[:flows].is_a?(Array) ? flow[:flows][0] : flow[:flows]
    data_set_hash = {}
    flow[:data_sets].each { |ds| data_set_hash[ds[:id]] = ds }
    flow[:data_sets] = self.order_provisioning_data_sets(flow_node, data_set_hash)
    flow.delete(:flows)

    data_source_id = nil
    data_source_id = flow[:data_sets][0][:data_source_id] if !flow[:data_sets][0].nil?

    if (data_source_id.nil?)
      ds_id = flow[:data_sets][0].nil? ? nil : flow[:data_sets][0][:id]
      logger.error("Invalid data flow, root data set missing source: data_sink: #{self.data_sink.id}, data_set: #{ds_id}")
      return nil
    end

    data_source = DataSource.find(data_source_id)

    if (data_source.nil?)
      logger.error("Data flow data_source missing: #{data_source_id}, data_sink: #{self.data_sink.id}")
      return nil
    end

    flow[:data_source] = {
      :id => data_source.id,
      :status => data_source.status,
      :source_config => data_source.source_config,
      :connection_type => data_source.source_type,
      :source_type => data_source.source_type
    }

    flow[:data_source][:data_credentials] = data_source.data_credentials.nil? ?
      nil : {
        :id => data_source.data_credentials.id,
        :credentials_enc => data_source.data_credentials.credentials_enc,
        :credentials_enc_iv => data_source.data_credentials.credentials_enc_iv
      }

    flow.delete(:data_sources)

    modified_data_sink = {
      :id => self.data_sink.id,
      :status => self.data_sink.status,
      :data_credentials_id => self.data_sink.data_credentials_id,
      :sink_config => self.data_sink.sink_config,
      :connection_type => data_sink.sink_type,
      :sink_type => data_sink.sink_type
    }

    modified_data_sink[:data_credentials] = self.data_sink.data_credentials.nil? ?
      nil : {
        :id => self.data_sink.data_credentials.id,
        :credentials_enc => self.data_sink.data_credentials.credentials_enc,
        :credentials_enc_iv => self.data_sink.data_credentials.credentials_enc_iv
      }
      
    flow[:data_sink] = modified_data_sink
    flow.delete(:data_sinks)

    return flow
  end

  def name
    resource.flow_name
  end

  def description
    resource.flow_description
  end

  def project (is_origin_flow = false)
    return self.resource&.origin_node&.project
  end

  def flows_quick (opts = { :all => false })
    raise Api::V1::ApiError.new(:internal_server_error, "Required api user missing") if self.user.blank?

    org_id = self.org&.id
    where_cnd = { :org_id => org_id }
    where_cnd[:owner_id] = self.user.id if !opts[:all]

    data_set_where = where_cnd.merge(self.params[:data_set_where] || {})
    ds = DataSet.where(data_set_where).select(:id, :data_source_id)

    data_source_where = where_cnd.merge(self.params[:data_source_where] || {})

    # Here we remove the :owner_id condition if a specific :id
    # condition was included in the caller's :data_source_where param
    data_source_where.delete(:owner_id) if params[:data_source_where].key?(:id)

    if (opts[:most_recent_limit].present?)
      # See NEX-10613. This work-around should be
      # removed once UI supports pagination in 
      # GET /flows and/or once GET /data_flows
      # support is removed.
      dsrc = DataSource.where(data_source_where)
        .order(updated_at: :desc)
        .limit(opts[:most_recent_limit])
    else
      dsrc = DataSource.where(data_source_where).select(:id)
    end
    dsrc_ids = dsrc.order(:id).pluck(:id)

    ds_src = Hash.new
    ds_children = Hash.new
    ds_derived = Array.new

    visited_ids = {
      :data_sets => Array.new
    }

    ds_ids = ds.map(&:id)
    children = DataSet.where(:parent_data_set_id => ds_ids)

    # Identify root data sets: those with a data source
    # or a parent data set shared with the user.

    ds.each do |d|
      next if (d.data_source_id.nil?)
      next if !dsrc_ids.include?(d.data_source_id)
      ds_src[d.data_source_id] ||= {}
      ds_src[d.data_source_id][d.id] = build_child_tree(d.id, children, visited_ids)
    end

    ds_from_shared = Hash.new
    dsf = DataSet.derived_from_shared_or_public(opts[:all] ? nil : self.user, self.org)

    dsf.each do |d|
      ds_from_shared[d.id] = build_child_tree(d.id, children, visited_ids)
    end

    dsets = DataSet.where(:id => visited_ids[:data_sets]).jit_preload
    sharers = {
      :sharers => DataSetsAccessControl.where(:data_set_id => visited_ids[:data_sets]).jit_preload,
      :external_sharers => ExternalSharer.where(:data_set_id => visited_ids[:data_sets]).jit_preload
    }

    data_sets_hash = Hash.new
    dsets.each {|d| data_sets_hash[d.id] = d }
    resources = DataFlow.empty_flows

    # Build flows originating in data sources.
    flow_params = {
      :data_sets_hash => data_sets_hash,
      :resources => resources,
      :sharer_acls => sharers,
      :owner_id => self.user.id,
      :org_id => org_id
    }

    ds_src.each do |dsrc_id, src_trees|
      src_trees.each do |ds_id, ds_tree|
        ds = data_sets_hash[ds_id]
        next if ds.nil?
        resources[:flows] << build_ds_flow(ds, ds_tree, nil, flow_params, opts)
        ds.add_origin_data_sinks(resources)
      end
    end

    # Build flows originating in shared data sets
    ds_from_shared.each do |ds_id, ds_tree|
      ds = data_sets_hash[ds_id]
      next if ds.nil?
      resources[:flows] << build_ds_flow_from_shared(ds, ds_tree, 
        ds.parent_data_set, flow_params, opts)
    end

    # Collect isolated data sources that have no
    # downstream data flows (i.e. no detected data sets).
    dsrc_ids = dsrc.select { |d| ds_src[d.id].nil? }.map(&:id)
    dsrcs = DataSource.where(:id => dsrc_ids).jit_preload
    dsrcs.each do |src|
      resources[:data_sources] << build_flow_resource_data(src, resources, opts)
    end

    tmp = Hash.new

    resources.each do |res_type, res|
      res_type = res_type.to_sym
      if (res_type == :flows)
        # Note, do not apply uniq to flows--the same
        # parent data set id may occur multiple times
        # as a root id in flows.
        tmp[res_type] = (res.is_a?(Array) ? res : [res])
      else
        tmp[res_type] = (res.is_a?(Array) ? res.compact.uniq {|r| r[:id] } : [res])
      end
    end

    pr_ids = []
    resources[:flows].each do |f|
      self.gather_flow_project_ids(f, pr_ids)
    end

    prs = []
    Project.where(id: pr_ids.compact.uniq).each do |p|
      pr = {
        :id => p.id,
        :name => p.name,
        :description => p.description,
        :access_roles => opts[:access_role].present? ? opts[:access_role] :
          p.get_access_roles(self.user, self.org)
      }
      prs << pr if !pr[:access_roles].empty?
    end
    tmp[:projects] = prs.uniq { |pr| pr[:id] }

    apply_flow_tags(tmp)
    apply_flow_docs(tmp)
    return tmp
  end

  def gather_flow_project_ids (f, pr_ids)
    pr_ids << f[:project_id] if f[:project_id].present?
    f[:children].each do |child|
      gather_flow_project_ids(child, pr_ids)
    end
  end

  def apply_flow_access_roles (resources)
    [
      CodeContainer,
      DataCredentials,
      DataSet,
      DataSink,
      DataSource
    ].each do |res_model|
      res_sym = res_model.name.underscore.pluralize.to_sym
      resources[res_sym].each do |res|
        self.update_access_roles_for_flow_accessor(res_model, res)
      end
    end

    resources[:projects].each do |prh|
      pr = Project.find(prh[:id])
      prh[:access_roles] = pr.get_access_roles(self.user, self.org)
    end

    resources[:projects] = resources[:projects].select { |pr| !pr[:access_roles].blank? }
  end

  def update_access_roles_for_flow_accessor (res_model, res)
    roles = (res[:access_roles] || Array.new)
    roles = roles.map(&:to_sym)

    1.times do
      break if self.user.nil?

      roles << :owner if (self.user.id == res[:owner_id])
      break if roles.include?(:owner)

      roles << :admin if self.is_admin?
      break if roles.include?(:admin)

      roles += self.flows_access_roles if self.flows_access_roles.present?

      if roles.empty?
        res_instance = res_model.find_by_id(res[:id])
        roles += res_instance.get_access_roles(self.user, self.org, false)
      end
    end

    res[:access_roles] = roles.uniq
  end

  def apply_flow_docs (flow)
    flow[:docs] = Array.new
    self.docs.each do |doc|
      flow[:docs] << {
        :id => doc.id,
        :name => doc.name,
        :description => doc.description,
        :doc_type => doc.doc_type,
        :repo_type => doc.repo_type,
        :created_at => doc.created_at,
        :updated_at => doc.updated_at
      }
    end
  end

  def apply_flow_tags (flow)
    models = [DataSource, DataSet, DataSink, DataCredentials]
    visited_ids = Hash.new

    models.each do |m|
      m_sym = m.table_name.to_sym
      visited_ids[m_sym] = flow[m_sym].pluck(:id)
    end

    taggings = Hash.new
    models.each do |m|
      m_sym = m.table_name.to_sym
      taggings[m_sym] = Tagging.where(:taggable_type => m.name,
        :taggable_id => visited_ids[m_sym])
    end

    all_tag_ids = Array.new
    models.each do |m|
      all_tag_ids += taggings[m.table_name.to_sym].map(&:tag_id)
    end   
   
    return if all_tag_ids.empty?

    all_tags = Hash.new
    Tag.where(:id => all_tag_ids).each { |t| all_tags[t.id] = t.name }

    models.each do |m|
      m_sym = m.table_name.to_sym
      tags = Hash.new
      taggings[m_sym].each do |t|
        tags[t.taggable_id] ||= Array.new
        tags[t.taggable_id] << all_tags[t.tag_id]
      end
      flow[m_sym].each do |res|
        res[:tags] = tags[res[:id]] || Array.new
      end
    end
  end

  protected

  def gather_code_container_ids (data_set)
    ids = [data_set.code_container_id]
    data_set.child_data_sets.each do |ds|
      next if (ds.org != self.org)
      ids += self.gather_code_container_ids(ds)
    end
    return ids
  end

  def order_provisioning_data_sets (flow_node, data_set_hash)
    return [] if flow_node.blank?
    ds = data_set_hash[flow_node[:id]]
    return [] if ds.nil?
    data_set = DataSet.find(ds[:id])
    return [] if data_set.nil?
    nds = {
      :id => data_set.id,
      :status => data_set.status,
      :data_source_id => data_set.data_source_id,
      :parent_data_set_id => data_set.parent_data_set_id,
      :transform => data_set.transform
    }
    return [nds] + order_provisioning_data_sets(flow_node[:children][0], data_set_hash)
  end

end
