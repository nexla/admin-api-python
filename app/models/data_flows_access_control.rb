class DataFlowsAccessControl < ApplicationRecord
  self.primary_key = :id

  include AuditLog

  def self.flows (opts)
    org_id = opts[:org]&.id
    status = opts[:status]&.upcase

    case opts[:accessor].class
    when Org
      ats = [
        { 
          :at => AccessControls::Accessor_Types[:org],
          :at_id => opts[:accessor].id
        }
      ]
    when Team
      ats = []
      if (!opts[:accessor].org.nil? && (opts[:accessor].org.id == org_id))
        ats << { 
          :at => AccessControls::Accessor_Types[:org],
          :at_id => opts[:accessor].org.id
        }
      end
      ats << {
        :at => AccessControls::Accessor_Types[:team],
        :at_id => opts[:accessor].id
      }
    else
      ats = []
      if (!opts[:org].nil?)
        ats << { 
          :at => AccessControls::Accessor_Types[:org],
          :at_id => opts[:org].id
        }
      end
      opts[:accessor].team_memberships.select { |tm| tm.team.org_id == org_id }.each do |tm|
        ats << {
          :at => AccessControls::Accessor_Types[:team],
          :at_id => tm.team.id
        }
      end
      ats << {
        :at => AccessControls::Accessor_Types[:user],
        :at_id => opts[:accessor].id,
        :org_id => org_id
      }
    end

    conditions = ats.map do |at|
      cnd = { :accessor_id => at[:at_id], :accessor_type => at[:at] }
      cnd[:accessor_org_id] = at[:org_id] if at.key?(:org_id)
      cnd
    end

    at_scope = DataFlowsAccessControl.where(conditions.shift)
    conditions.each do |condition|
      at_scope = at_scope.or( DataFlowsAccessControl.where(condition) )
    end

    dfs = at_scope.to_a.select { |df| df.enables_role?(opts[:access_role]) }

    if status.present?
      dfs = dfs.select { |df| (df.resource.status == status) }
    end

    all_flows = dfs.map do |df|
      next if df.resource.nil?
      user = opts[:accessor].is_a?(User) ? opts[:accessor] : df.resource.owner
      df.data_flow(opts[:accessor], opts[:org]).flows(
        !!opts[:downstream_only],
        !!opts[:full_tree]
      )
    end.flatten

    DataFlow.merge_flows(all_flows)
  end

  def self.flows_quick (opts)
    org_id = opts[:org]&.id
    status = opts[:status]&.upcase

    case opts[:accessor].class
    when Org
      ats = [
        { 
          :at => AccessControls::Accessor_Types[:org],
          :at_id => opts[:accessor].id
        }
      ]
    when Team
      ats = []
      if (!opts[:accessor].org.nil? && (opts[:accessor].org.id == org_id))
        ats << { 
          :at => AccessControls::Accessor_Types[:org],
          :at_id => opts[:accessor].org.id
        }
      end
      ats << {
        :at => AccessControls::Accessor_Types[:team],
        :at_id => opts[:accessor].id
      }
    else
      ats = []
      if (!opts[:org].nil?)
        ats << { 
          :at => AccessControls::Accessor_Types[:org],
          :at_id => opts[:org].id
        }
      end
      opts[:accessor].team_memberships.select { |tm| tm.team.org_id == org_id }.each do |tm|
        ats << {
          :at => AccessControls::Accessor_Types[:team],
          :at_id => tm.team.id
        }
      end
      ats << {
        :at => AccessControls::Accessor_Types[:user],
        :at_id => opts[:accessor].id,
        :org_id => org_id
      }
    end

    data_source_ids = {}
    ats.each do |at|
      cnd = { :accessor_id => at[:at_id], :accessor_type => at[:at] }
      cnd[:accessor_org_id] = at[:org_id] if at.key?(:org_id)

      dfs = DataFlowsAccessControl.where(cnd)
        .select { |df| df.enables_role?(opts[:access_role]) }

      if !status.nil?
        dfs = dfs.select { |df| (df.resource.status == status) }
      end

      dfs.each do |df|
        data_source_ids[df.resource.owner.id] ||= { owner: df.resource.owner, ids: Array.new }
        data_source_ids[df.resource.owner.id][:ids] << df.data_source_id
      end
    end

    all_flows = []
    data_source_ids.keys.each do |k|
      flows_params = {
        :user => data_source_ids[k][:owner],
        :org => opts[:org],
        :data_source_where => { :id => data_source_ids[k][:ids].compact },
        :data_set_where => {}
      }
      all_flows << DataFlow.new(flows_params).flows_quick
    end

    return DataFlow.merge_flows(all_flows)
  end

  def accessor
    ac = nil
    return ac if self.accessor_id.nil?
    case self.accessor_type
    when AccessControls::Accessor_Types[:user]
      ac = User.find_by_id(self.accessor_id)
    when AccessControls::Accessor_Types[:team]
      ac = Team.find_by_id(self.accessor_id)
    when AccessControls::Accessor_Types[:org]
      ac = Org.find_by_id(self.accessor_id)
    end
    ac
  end
  
  def enables_role? (access_role)
    DataFlow.access_roles_enable_role?(self.role_index, access_role)
  end

  def get_access_roles
    [ AccessControls::ALL_ROLES_SET[self.role_index] ]
  end

  def resource
    return DataSource.find_by_id(self.data_source_id) if !self.data_source_id.nil?
    return DataSink.find_by_id(self.data_sink_id) if !self.data_sink_id.nil?
    return DataSet.find_by_id(self.data_set_id)
  end

  def data_flow (user = nil, org = nil)
    if (user.nil?)
      user = self.resource.owner
      org = self.resource.org
    end

    key = self.resource.class.name.underscore.to_sym
    sub_key = "#{resource.id}:#{user.id}:" + (org.nil? ? "0" : org.id.to_s)
    flows_cache = RequestStore[:flows]
    flow = flows_cache.nil? ? nil : flows_cache[key][sub_key]

    if flow.blank?
      flow = DataFlow.new({
        :data_source_id => self.data_source_id,
        :data_sink_id => self.data_sink_id,
        :data_set_id => self.data_set_id,
        :user => user,
        :org => org
      })
      flows_cache[key][sub_key] = flow if !flows_cache.nil?
    end

    return flow
  end
end
