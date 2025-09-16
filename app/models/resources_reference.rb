class ResourcesReference < ApplicationRecord
  belongs_to :org
  belongs_to :referencing, polymorphic: true
  belongs_to :referenced, polymorphic: true

  scope :data_sets_refs, -> { where(referenced_type: 'DataSet') }
  scope :data_sources_refs, -> { where(referenced_type: 'DataSource') }
  scope :data_credentials_refs, -> { where(referenced_type: 'DataCredentials') }
  scope :data_maps_refs, -> { where(referenced_type: 'DataMap') }
  scope :data_sinks_refs, -> { where(referenced_type: 'DataSink') }
  scope :code_containers_refs, -> { where(referenced_type: 'CodeContainer') }
  scope :runtimes_refs, -> { where(referenced_type: 'Runtime') }

  # stores origins for referencing item to later resolve ACL for referenced item
  has_many :resources_references_origins, dependent: :destroy
  alias_method :origins, :resources_references_origins

  has_many :origin_nodes, through: :resources_references_origins, source: :origin_node, class_name: 'FlowNode'

  before_save :handle_before_save
  after_save :ensure_origin_nodes

  def self.origin_nodes_ids_for(referenced)
    ResourcesReference.where(referenced: referenced).joins(:resources_references_origins).pluck(:origin_node_id).uniq
  end

  def referencing_resource
    self.referencing_type.constantize.find_by_id(self.referencing_id)
  end

  def referenced_resource
    self.referenced_type.constantize.find_by_id(self.referenced_id)
  end

  protected

  def handle_before_save
    if self.org_id.nil?
      self.org_id = self.referencing_resource&.org_id
    end
  end

  def ensure_origin_nodes
    referencing_origin_node_ids = if referencing.respond_to?(:origin_node_id)
                                    [referencing.origin_node_id]
                                  elsif referencing.respond_to?(:origin_nodes)
                                    referencing.origin_nodes.map(&:id)
                                  end
    referencing_origin_node_ids ||= []
    referencing_origin_node_ids = referencing_origin_node_ids.compact.uniq

    existing_origin_node_ids = self.origins.map(&:origin_node_id)
    new_origin_node_ids = referencing_origin_node_ids - existing_origin_node_ids
    new_origin_node_ids.each { |origin_node_id| self.origins.create!(origin_node_id: origin_node_id) }
  end
end