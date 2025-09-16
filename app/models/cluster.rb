class Cluster < ApplicationRecord
  self.primary_key = :id

  include AuditLog
  include Api::V1::Schema

  belongs_to :org
  has_many :cluster_endpoints, dependent: :destroy

  Statuses = {
    :init => 'INIT',
    :active => 'ACTIVE',
    :paused => 'PAUSED'
  }

  class ClusterValidator < ActiveModel::Validator
    def validate (cluster)
      dc = Cluster.default_cluster
      if (cluster.is_default? && dc.present? && (dc.id != cluster.id))
        cluster.errors.add :base, "Cannot create default cluster while one already exists"
        cluster.status = :forbidden
      end
      if cluster.uid.present?
        c = Cluster.find_by_uid(cluster.uid)
        if c.present? && (c.id != cluster.id)
          cluster.errors.add :base, "A cluster with uid #{cluster.uid} already exists"
          cluster.status = :forbidden
        end
      end
    end
  end

  validates_with ClusterValidator

  def self.statuses_enum
    enum = "ENUM("
    first = true
    Statuses.each do |k, v|
      enum += "," if !first
      enum += "'#{v}'"
      first = false
    end
    enum + ")"
  end

  def self.default_cluster
    return unless connection.table_exists?(:clusters)

    Cluster.find_by(is_default: true)
  end

  def self.default_endpoint (service)
    return unless connection.table_exists?(:clusters)

    Cluster.default_cluster&.cluster_endpoints&.find_by(service: Initializers::DataplaneServices::Services[service])
  end

  def self.default_service (service)
    # NOTE this method gets the service entry loaded from configuration
    # at API boot time, NOT the entry from cluster_endpoints. This is
    # used in RedisService and SearchService::BaseSearch
    return nil if !Cluster.default_cluster.present?
    services = DATAPLANE_SERVICES.find_services(Cluster.default_cluster&.dataplane_key)
    return services.is_a?(Hash) ? services[service] : nil
  end

  def self.get_infrastructure_access_key (uid = nil)
    uid ||= Cluster.default_cluster&.dataplane_key
    secrets = API_SECRETS.find_dataplane_secrets(uid)
    return secrets.blank? ? nil : secrets[:infrastructure_access_key]
  end

  def self.validate_cluster_for_org (cluster_id, org_id = nil)
    raise Api::V1::ApiError.new(:bad_request, "A cluster id is required") if cluster_id.nil?

    cluster = Cluster.find_by(id: cluster_id)
    raise Api::V1::ApiError.new(:bad_request, "Cluster not found: #{cluster_id}") if cluster.blank?

    if org_id.nil?
      # New orgs must be assigned to an active, public
      # cluster owned by Nexla. That's usually the
      # default cluster for the environment.
      return if cluster.available?
      raise Api::V1::ApiError.new(:bad_request,
        "Cluster is either private or not active: #{cluster.name}, #{cluster.id}")
    end

    # In this case the org is being assigned to a
    # private cluster that it owns. That's ok.
    return if (org_id == cluster.org_id)

    # Check if we are assigning the org to
    # to an available Nexla cluster...
    return if cluster.available?
  
    raise Api::V1::ApiError.new(:bad_request, "Cannot assign org to cluster: #{cluster_id}")
  end

  def self.get_base_url (org, service, config, accept_header = :json, header_org = nil, content_type = :json)
    if (service.nil?)
      header = get_request_header(nil, accept_header, header_org, content_type)
      return config[:base_url], header, nil
    end

    cluster_endpoint = get_cluster_endpoint(org, service)
    header_val = get_request_header(cluster_endpoint, accept_header, header_org, content_type)
    header_host = header_val.key?(:Host) ? header_val[:Host] : nil

    if cluster_endpoint.present?
      base_url = UrlBuilder.new(cluster_endpoint).base_url
      return base_url, header_val, header_host
    end

    return config[:base_url], header_val, header_host
  end

  def self.get_cluster_endpoint (org, service)
    service_name = Initializers::DataplaneServices::Services[service]
    cluster = (org.nil? || org.cluster.nil?) ? Cluster.default_cluster : org.cluster
    return nil if !cluster.present?
    return cluster.cluster_endpoints.find_by(service: service_name)
  end

  def self.get_service_url (org, service)
    cluster_endpoint = get_cluster_endpoint(org, service)
    return "" if cluster_endpoint.blank?
    base_url = UrlBuilder.new(cluster_endpoint).base_url
    base_url
  end

  def self.get_request_header (cluster_endpoint, accept_header, header_org, content_type)
    header = header_org.nil? ? {} : header_org
    header[:content_type] = content_type.to_s
    header[:Authorization] = cluster_endpoint.cluster.authorization_header
    header[:accept] = accept_header.to_s
    header[:Host] = cluster_endpoint.header_host if cluster_endpoint.present? && cluster_endpoint.header_host.present?
    header
  end

  def self.build_from_input (input, endpoints)
    if (input[:is_default].truthy? && !Cluster.default_cluster.nil?)
      raise Api::V1::ApiError.new(:conflict, "A default cluster already exists")
    end

    org = Org.find(input[:org_id])
    if (input.key?(:is_private) && !input[:is_private].truthy?)
      raise Api::V1::ApiError.new(:bad_request,
        "Public clusters must belong to Nexla org") if !org.is_nexla_admin_org?
    end

    cluster = nil
    self.transaction do
      begin
        cluster = Cluster.create(input)
      rescue ActiveRecord::RecordNotUnique
        raise Api::V1::ApiError.new(:conflict, "Cluster with name '#{input[:name]}' already exists")
      end
      
      if (endpoints.is_a?(Array))
        endpoints.each do |endpoint|
          endpoint[:cluster_id] = cluster.id
          endpoint[:org_id] = cluster.org.id
          ClusterEndpoint.create(endpoint)
        end
      end

      cluster.save!
    end

    cluster
  end

  def update_mutable! (input, endpoints)
    if !input[:is_default].nil?
      default_cluster = Cluster.default_cluster
      if !default_cluster.nil?
        raise Api::V1::ApiError.new(:conflict, "A default cluster already exists") if input[:is_default] and default_cluster.id != self.id
        raise Api::V1::ApiError.new(:conflict, "Cannot change a cluster to non-default") if !input[:is_default] and default_cluster.id == self.id
      end
      self.is_default = input[:is_default]
    end

    self.region = input[:region] if !input[:region].blank?
    self.provider = input[:provider] if !input[:provider].blank?
    self.org_id = input[:org_id] if input.key?(:org_id)

    self.transaction do
      begin
        endpoints.each do |ep|
          existing_ep = self.cluster_endpoints.find { |e| e.service == ep[:service] }
          if existing_ep.present?
            # Do not allow changing cluster id of
            # cluster endpoint in this path. Use
            # PUT /cluster_endpoints instead.
            ep.delete(:cluster_id)
            existing_ep.update_mutable!(ep)
          else
            ep[:cluster_id] = self.id
            ep[:org_id] = self.org.id
            ClusterEndpoint.create(ep)
          end
        end if (endpoints.is_a?(Array))
        self.save!
      end
    end
  end

  def dataplane_key
    self.uid&.downcase
  end

  def supports_multi_dataplane?
    self.dataplane_key != self.name&.downcase
  end

  def available?
    # A cluster is available if it belongs to Nexla,
    # is active, and is not private. Non-Nexla orgs
    # can be assigned to available clusters for their
    # default data plane. Note: unprotected dereference
    # of get_nexla_admin_org. We want that to raise
    # an exception if the org is missing.
    return (self.org.id == Org.get_nexla_admin_org.id) &&
      self.active? && !self.is_private?
  end

  def active?
    (self.status == Statuses[:active])
  end

  def activate!
    # SHOULD check cluster validity here...
    # does it have valid cluster endpoints?
    self.status = Statuses[:active]
    self.save!
  end

  def paused?
    (self.status == Statuses[:paused])
  end

  def pause!
    self.status = Statuses[:pause]
    self.save!
  end

  def set_default
    if !self.available?
      raise Api::V1::ApiError.new(:bad_request,
        "Private or inactive cluster cannot be made default: #{self.name}, #{self.id}")
    end
    self.transaction do
      current_default = Cluster.default_cluster
      if (current_default.id != self.id)
        current_default.is_default = false
        current_default.save!
        self.is_default = true
        self.save!
      end
    end
  end

  def authorization_header
    secrets = API_SECRETS.find_dataplane_secrets(self.dataplane_key)
    secrets ||= API_SECRETS.find_dataplane_secrets(Cluster.default_cluster.dataplane_key)
    secrets ||= {}
    return "Basic " + Base64.encode64("#{secrets[:username]}:#{secrets[:password]}").gsub("\n", "")
  end

  def script_config (resource_type)
    key = ("script_" + resource_type.to_s.sub("data_", "")).to_sym
    services = DATAPLANE_SERVICES.find_services(self.dataplane_key)
    return (services.blank? || services[key].blank?) ? {} : services[key]
  end
end
