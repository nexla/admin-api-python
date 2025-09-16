class Project < ApplicationRecord
  include Api::V1::Schema
  include AccessControls::Standard
  include Accessible
  include AuditLog
  include Copy
  include Docs
  include SearchableConcern

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :copied_from, class_name: "Project", foreign_key: "copied_from_id"

  has_many :projects_data_flows, dependent: :destroy
  has_many :flow_nodes

  acts_as_taggable_on :tags
  def tags_list
    self.tags.pluck(:name)
  end
  alias_method :tag_list, :tags_list

  def self.accessible_origin_nodes (accessor, access_role, org, scope = nil)
    return FlowNode.none if !accessor.respond_to?(:projects)
    if (Project.access_roles.include?(access_role) || (access_role == :all))
      ps = accessor.is_a?(User) ? accessor.projects(org, access_role: access_role) :
        accessor.projects(access_role, org)
      return FlowNode.where(id: ps.map { |p| p.flow_nodes.pluck(:id) }.flatten.uniq
      )
    else
      return FlowNode.none
    end
  end

  def self.build_from_input (api_user_info, input)
    if (input.blank? || api_user_info.input_owner.nil?)
      raise Api::V1::ApiError.new(:bad_request, "Project input missing")
    end

    project = Project.new
    Project.transaction do
      project.owner = api_user_info.input_owner
      project.org = api_user_info.input_org
      project.update_mutable!(api_user_info, input)
    end

    return project
  end

  def update_mutable! (api_user_info, input)
    return if (input.blank? || api_user_info.nil?)

    self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
    self.org = api_user_info.input_org if (self.org != api_user_info.input_org)
    self.name = input[:name] if input.key?(:name)

    tags = input.delete(:tags)

    if self.name.blank?
      raise Api::V1::ApiError.new(:bad_request, "Project name missing")
    end

    self.description = input[:description] if input.key?(:description)
    self.client_identifier = input[:client_identifier] if input.key?(:client_identifier)
    self.client_url = input[:client_url] if input.key?(:client_url)
    self.save!

    if (input.key?(:flows))
      # We support only updating of associated data flows
      # in POST or PUT requests. To remove or reset data
      # flow associations, the caller must use the specific
      # endpoints (e.g. DELETE /projects/<id>/flows)
      self.update_flows(input[:flows], api_user_info)
    elsif input.key?(:data_flows)
      # FN backwards-compatibility
      self.update_data_flows(input[:data_flows], api_user_info)
    end

    ResourceTagging.add_owned_tags(self, { tags: tags }, api_user_info.input_owner)
  end

  def update_data_flows (data_flows, api_user_info)
    # FN backwards-compatibility
    flow_node_ids = []
    data_flows.each do |df|
      # Note, input json validation has already checked
      # that we received an array of generic objects. So
      # we can assume df.is_a?(Hash)
      df.symbolize_keys!
      res_key = (df.keys & DataFlow::Resource_Keys).first
      next if (res_key.nil? || df[res_key].blank?)

      res_id = df[res_key]
      res = DataFlow.resource_key_model(res_key)&.find_by_id(res_id)
      if res.nil?
        raise Api::V1::ApiError.new(:bad_request,
          "Data flow cannot be added to project: #{res_key}, #{res_id}")
      end
      flow_node_ids << res.flow_node_id
    end
    self.update_flows(flow_node_ids, api_user_info)
  end

  def reset_flows (flow_node_ids, api_user_info)
    self.update_flows(flow_node_ids, api_user_info, true)
  end

  def update_flows (flow_node_ids, api_user_info, reset = false)
    ability = Ability.new(api_user_info.user)
    origin_nodes = FlowNode.origin_nodes(flow_node_ids)

    origin_nodes.each do |fn|
      if (!ability.can?(:manage, fn))
        raise Api::V1::ApiError.new(:forbidden, "Invalid access to flow: #{fn.id}")
      end
    end

    self.transaction do
      if (reset)
        # Note, it would be more efficient to use update_all() here,
        # but that would not generate entries flow_node_versions.
        self.flow_nodes.each do |fn|
          fn.project_id = nil
          fn.save!
        end
      end

      origin_nodes.each do |fn|
        fn.project_id = self.id
        fn.save!
      end
    end

    self.reload
  end

  def remove_flows (flow_node_ids)
    origin_nodes = FlowNode.origin_nodes(flow_node_ids)
    count = 0

    # We don't check whether the caller can manage
    # the flows listed in flow_node_ids here because
    # we assume the caller can manage the project
    # (projects_controller.rb checks that). You do
    # need admin access to ADD a flow to project.
    # Otherwise, you could promote yourself to admin
    # on a flow just by adding it to your project.

    origin_nodes.each do |fn|
      next if (fn.project_id != self.id)
      fn.project_id = nil
      fn.save!
      count += 1
    end

    self.reload if (count > 0)
  end

  def destroy
    self.transaction do
      self.flow_nodes.each do |f|
        f.project_id = nil
        f.save!
      end
      super
    end
  end
end
