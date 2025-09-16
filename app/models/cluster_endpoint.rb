class ClusterEndpoint < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AuditLog

  belongs_to :cluster
  belongs_to :org

  def self.services_enum
    "ENUM(" + Initializers::DataplaneServices::Services.values.map{|v| "'#{v}'"}.join(",") + ")"
  end

  def self.build_from_input (input)
    input[:org_id] = Cluster.find(input[:cluster_id]).try(:org_id)
    return ClusterEndpoint.create(input)
  end

  def update_mutable! (input)
    return if input.nil?

    if (!input[:cluster_id].blank?)
      self.cluster_id = input[:cluster_id]
      self.org_id = Cluster.find(self.cluster_id)
    end

    self.service = input[:service] if !input[:service].blank?
    self.protocol = input[:protocol] if !input[:protocol].blank?
    self.host = input[:host] if !input[:host].blank?
    self.port = input[:port] if !input[:port].blank?
    self.context = input[:context] if !input[:context].blank?
    self.save!
  end

end
