class CodeContainer < ApplicationRecord
  self.primary_key = :id

  DESCRIPTION_LIMIT = 255

  include Api::V1::Schema
  include AccessControls::Standard
  include Accessible
  include JsonAccessor
  include AuditLog
  include FlowNodeData
  include Copy
  include Docs
  include SearchableConcern

  include ReferencedResourcesConcern

  belongs_to :owner,
    class_name: "User",
    foreign_key: "owner_id",
    required: true
  belongs_to :org
  belongs_to :data_credentials
  belongs_to :copied_from,
    class_name: "CodeContainer",
    foreign_key: "copied_from_id"
  belongs_to :runtime_data_credentials,
    class_name: "DataCredentials",
    foreign_key: "runtime_data_credentials_id"

  has_many :code_containers_data_maps, dependent: :destroy
  has_many :data_maps, through: :code_containers_data_maps, source: :data_map
  has_many :data_sets
  has_many :data_sources
  has_many :dashboard_transforms

  referencing_resources :data_maps, :data_credentials, :data_sets, :code_containers
  mark_as_referenced_resource

  before_save :handle_before_save
  after_save :identify_data_maps
  after_commit :after_commit

  json_accessor :code_config, :code, :custom_config, :repo_config

  validates_length_of :name, maximum: DESCRIPTION_LIMIT
  validates_length_of :description, maximum: DESCRIPTION_LIMIT

  attr_accessor :code_error

  acts_as_taggable_on :tags
  def tags_list
    self.tags.pluck(:name)
  end
  alias_method :tag_list, :tags_list

  scope :reusable,      -> { where(:reusable => true) }
  scope :not_reusable,  -> { where(:reusable => false) }
  scope :public_scope, -> { where(public: true) } # public is reserved name

  attr_accessor :emit_update_messages

  after_initialize do
    self.emit_update_messages = false
  end

  Encoding_Types = {
    :none    => "none",
    :base64  => "base64"
  }

  Resource_Types = API_CODE_CONTAINER_RESOURCE_TYPES
  Code_Languages = API_CODE_LANGUAGES
  Code_Types = API_CODE_TYPES
  Output_Types = API_OUTPUT_TYPES

  Repo_Types = API_DOC_REPO_TYPES

  Ai_Function_Types = API_AI_FUNCTIONS_TYPES

  def self.resource_types_enum
    "ENUM(" + Resource_Types.values.map{|v| "'#{v}'"}.join(",") + ")"
  end

  def self.code_types_enum
    "ENUM(" + Code_Types.values.map{|v| "'#{v}'"}.join(",") + ")"
  end

  def self.output_types_enum
    "ENUM(" + Output_Types.values.map{|v| "'#{v}'"}.join(",") + ")"
  end

  def self.build_from_input (api_user_info, input)
    if (input.blank? || api_user_info.input_owner.nil?)
      raise Api::V1::ApiError.new(:bad_request, "Code container input missing")
    end

    input[:owner_id] = api_user_info.input_owner.id
    input[:org_id] = api_user_info.input_org.nil? ? nil : api_user_info.input_org.id

    if (input.key?(:data_credentials_id))
      data_credentials = DataCredentials.find_by(id: input[:data_credentials_id])
      unless data_credentials
        raise Api::V1::ApiError.new(:not_found, "Data credentials not found")
      end

      if (!Ability.new(api_user_info.input_owner).can?(:read, data_credentials))
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data credentials")
      end
    end

    if (input.key?(:runtime_data_credentials_id))
      runtime_data_credentials = DataCredentials.find(input[:runtime_data_credentials_id])
      if (!Ability.new(api_user_info.input_owner).can?(:read, runtime_data_credentials))
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to runtime data credentials")
      end
    end

    tags = input[:tags]
    input.delete(:tags)

    if input[:resource_type] == Resource_Types[:ai_function] && input[:ai_function_type].blank?
      raise Api::V1::ApiError.new(:bad_request, "AI function type is required")
    end

    if input[:resource_type] == Resource_Types[:ai_function] && input[:output_type].blank?
      input[:output_type] = Output_Types[:custom]
    end

    if input[:resource_type] == Resource_Types[:splitter]
      raise Api::V1::ApiError.new(:bad_request, "Splitter cannot be created directly. Please create a Nexset with transform.")
    end

    code_container = nil
    CodeContainer.transaction do
      ref_fields = input.delete(:referenced_resource_ids)
      code_container = CodeContainer.new(input)
      code_container.verify_ref_resources!(api_user_info.input_owner, ref_fields)

      code_container.save!

      code_container.update_referenced_resources(ref_fields)
      code_container.update_external_repo(input) if code_container.has_external_repo?
      v = CodeUtils.validate_code_container(code_container) if code_container.resource_type != Resource_Types[:source_custom]
      raise Api::V1::ApiError.new(:bad_request, v[:description]) if !v.nil?
    end

    ResourceTagging.add_owned_tags(code_container, { :tags => tags }, api_user_info.input_owner)
    return code_container
  end

  def update_mutable! (api_user_info, input)
    return if (input.blank? || api_user_info.nil?)

    if self.ai_function_type.present? && self.public?
      raise Api::V1::ApiError.new(:method_not_allowed, "Public AI functions cannot be updated") unless input.key?(:public) && !input[:public].truthy?
    end

    ref_fields = input.delete(:referenced_resource_ids)
    verify_ref_resources!(api_user_info.input_owner, ref_fields)

    tags = input.delete(:tags)

    self.name = input[:name] if input.key?(:name)
    self.description = input[:description] if input.key?(:description)
    self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
    self.org = api_user_info.input_org if (self.org != api_user_info.input_org)

    if (input.key?(:data_credentials_id))
      data_credentials = DataCredentials.find(input[:data_credentials_id])
      if (!Ability.new(api_user_info.input_owner).can?(:read, data_credentials))
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data credentials")
      end
      self.data_credentials = data_credentials
    end

    if (input.key?(:runtime_data_credentials_id))
      runtime_data_credentials = DataCredentials.find(input[:runtime_data_credentials_id])
      if (!Ability.new(api_user_info.input_owner).can?(:read, runtime_data_credentials))
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to runtime data credentials")
      end
      self.runtime_data_credentials = runtime_data_credentials
    end

    if input.key?(:reusable)
      if self.resource_type == Resource_Types[:splitter] && input[:reusable].truthy?
        raise Api::V1::ApiError.new(:bad_request, "Splitter transform cannot be reusable")
      end

      self.reusable = input[:reusable]
    end
    if (input.key?(:public))
      raise Api::V1::ApiError.new(:method_not_allowed, "Input cannot include public attribute") if !api_user_info.user.super_user?
      self.public = input[:public]
    end

    if !input[:resource_type].blank?
      if self.resource_type != Resource_Types[:splitter] && input[:resource_type] == Resource_Types[:splitter]
        raise Api::V1::ApiError.new(:bad_request, "Cannot change resource type to splitter")
      end

      self.resource_type   = input[:resource_type]
    end

    self.output_type     = input[:output_type] if !input[:output_type].blank?
    self.code_type       = input[:code_type] if !input[:code_type].blank?
    self.code_encoding   = input[:code_encoding] if !input[:code_encoding].blank?
    self.code_config     = input[:code_config] if input.key?(:code_config)
    self.repo_config     = input[:repo_config] if input.key?(:repo_config)
    self.custom_config   = input[:custom_config] if input.key?(:custom_config)
    self.code            = parse_code_value(input[:code]) if input.key?(:code)

    self.repo_type       = input[:repo_type] if input.key?(:repo_type)

    if self.repo_type.present? && !self.repo_type.in?(Repo_Types.values)
      raise Api::V1::ApiError.new(:bad_request, "Invalid repo type")
    end

    v = CodeUtils.validate_code_container(self) if self.resource_type != Resource_Types[:source_custom]
    raise Api::V1::ApiError.new(:bad_request, v[:description]) if !v.nil?

    self.transaction do
      self.update_referenced_resources(ref_fields)
      if self.resource_type == Resource_Types[:splitter] && self.data_sets.present? && self.code_changed?
        splitter_tx = self.data_sets.first.update_splitter_children(self.code)
        self.code = splitter_tx if splitter_tx.present?
      end
      self.save!
      self.update_external_repo(input) if self.has_external_repo?

      ResourceTagging.add_owned_tags(self, { tags: tags }, api_user_info.input_owner)
    end

  end

  def update_external_repo(input)
    if input.key?(:code)
      github = GithubService.new(self)
      begin
        content = (github.get_file_info(self.repo_config["branch"]) rescue nil)
        resp = if content.present?
                 github.update_code(input[:code], content[:sha], input[:message])
               else
                 github.create_code(input[:code], input[:message])
               end

        unless self.repo_config["ref"].to_s.upcase == GithubService::HEAD
          self.repo_config = self.repo_config.merge({ "ref" => resp[:commit][:sha] })
        end
      rescue StandardError => e
        set_code_error(e.message)
      end
    end
    self.save!
  end

  def parse_code_value(input_code)
    self.has_external_repo? ? {} : input_code
  end

  def code_value(input)
    if self.is_jolt_custom?
      code_obj = input[:code].find { |c| c["operation"] == "nexla.custom" }
      code_obj.dig("spec","script")
    else
      input[:code]
    end
  end

  def public=(is_public)
    if (!!is_public)
      self.reusable = true
      super(true)
    else
      super(false)
    end
  end

  def has_external_repo?
    repo_type == Repo_Types[:github]
  end

  def is_validator?
    (self.resource_type == Resource_Types[:validator])
  end

  def is_output_record?
    (self.output_type == Output_Types[:record])
  end

  def is_output_attribute?
    (self.output_type == Output_Types[:attribute])
  end

  def is_output_custom?
    (self.output_type == Output_Types[:custom])
  end

  def is_base64?
    (code_encoding == Encoding_Types[:base64])
  end

  def is_jolt?
    is_jolt_standard? || is_jolt_custom?
  end

  def is_jolt_standard?
    self.code_type == Code_Types[:jolt_standard]
  end

  def is_jolt_custom?
    self.code_type == Code_Types[:jolt_custom]
  end

  def is_script?
    [:python, :python3, :javascript, :javascript_es6].any? { |type| self.code_type == Code_Types[type] }
  end

  def is_sql?
    Code_Types.values_at(:flink_sql, :spark_sql).include?(code_type)
  end

  def code_language
    language = nil

    path = self.repo_config["path"]
    if path.present?
      extension = path.split(".").last
      case extension
      when "py"
        language = Code_Languages[:python]
      when "js"
        language = Code_Languages[:javascript]
      when "sql"
        language = Code_Languages[:sql]
      end
    else
      case self.code_type
      when Code_Types[:jolt_custom], Code_Types[:jolt_standard]
        language = Code_Languages[:jolt]
      when Code_Types[:python], Code_Types[:python3], Code_Types[:javascript], Code_Types[:javascript_es6]
        language = self.code_type
      when Code_Types[:flink_sql], Code_Types[:spark_sql]
        language = Code_Languages[:sql]
      end
    end

    language
  end

  def get_code
    kode = self.code
    if (kode.empty? && self.has_external_repo?)
      github = GithubService.new(self)
      begin
        kode = github.get_code
      rescue StandardError => e
        set_code_error(e.message)
      end
    end

    kode = kode.deep_symbolize_keys if kode.is_a?(Hash)

    if self.is_jolt_custom? && !kode.is_a?(Array)
      kode = [
        {
          "operation": "nexla.custom",
          "spec": {
            "language": self.code_language,
            "encoding": self.code_encoding,
            "script": kode
          }
        }
      ]
    end
    kode
  end

  def code_changed?
    (self.code_config_changed? || super)
  end

  def maybe_destroy
    self.reload
    self.destroy if self.data_sets.empty? && !self.reusable?
  end

  def destroy
    self.data_sets.reload
    if (!self.data_sets.empty?)
      ids = self.data_sets.map(&:id)
      raise Api::V1::ApiError.new(:method_not_allowed, "Code container is in use by one or more data sets: #{ids.inspect}")
    end

    if (!self.dashboard_transforms.empty?)
      ids = self.dashboard_transforms.map(&:id)
      raise Api::V1::ApiError.new(:method_not_allowed, "Code container is in use by one or more dashboard transforms: #{ids.inspect}")
    end

    if self.ai_function_type.present? && self.public?
      raise Api::V1::ApiError.new(:method_not_allowed, "Public AI functions cannot be deleted")
    end
    super
  end

  def self.create_resource_script_config(input, config, resource, user, org, resource_type)
    input[:script_config].symbolize_keys!
    code_config = config || {}
    raise Api::V1::ApiError.new(:bad_request, "Code container missing required code_config or code") if (code_config.empty?)

    if resource.code_container.nil? || resource.code_container.reusable?
      code_container = CodeContainer.new
    else
      code_container = resource.code_container
    end
    code_container.owner = user
    code_container.org = org

    name = input[:script_config][:name] || "Script Config"
    code_container.code_config = code_config
    code_container.name = name
    code_container.reusable = false
    code_container.output_type     = input[:script_config][:output_type] if !input[:script_config][:output_type].blank?
    code_container.code_encoding   = input[:script_config][:code_encoding] if !input[:script_config][:code_encoding].blank?
    code_container.resource_type   = resource_type
    code_container.code_type       = input[:script_config][:code_type] if !input[:script_config][:code_type].blank?
    code_container.save!
    return code_container
  end

  def branches
    if self.repo_config.present?
      begin
        GithubService.new(self).branches
      rescue StandardError => e
        set_code_error(e.message)
      end
    end
  end

  def flow_attributes (user, org)
    [
      :data_credentials_id,
      :public,
      :managed,
      :reusable,
      :resource_type,
      :output_type,
      :code_type,
      :code_encoding
    ].map { |attr| [attr, self.send(attr)] }
  end

  def origin_node_ids
    # Note, multiple data_sets with the same origin node can have the same
    # code_container_id (where code_container.reusable == true). Hence, .distinct()
    ids = DataSet.where(code_container_id: self.id)
      .select(:origin_node_id).distinct.pluck(:origin_node_id)
    ids << ResourcesReference.origin_nodes_ids_for(self)
    ids.flatten.uniq
  end

  def origin_nodes
    FlowNode.where(id: self.origin_node_ids)
  end

  def available_as_reranker?
    return false if self.resource_type != Resource_Types[:ai_function]
    return false if self.ai_function_type != Ai_Function_Types[:reranker]

    data_sets.empty?
  end

  def self.load_from_config
    admin = User.find_by_email("admin@#{Org::Nexla_Admin_Email_Domain}")
    org = Org.get_nexla_admin_org
    return if org.blank? || admin.blank?

    return unless ActiveRecord::Base.connection.table_exists?(CodeContainer.table_name)
    specs = JSON.parse(File.read("#{Rails.root}/config/api/public_code_containers.json"))
    specs.each do |spec|
      next if CodeContainer.find_by(name: spec["name"]).present?

      spec["owner_id"] = admin.id
      spec["org_id"] = org.id
      CodeContainer.create!(spec)
    end
  end

  protected

  def set_code_error(message)
    self.code_error ||= ''
    if self.code_error.present?
      self.code_error += "\n"
    end
    self.code_error += message
  end

  def handle_before_save
    self.emit_update_messages = (
      self.code_type_changed? ||
      self.code_encoding_changed? ||
      self.code_config_changed? ||
      self.custom_config_changed? ||
      self.code_changed? ||
      self.repo_config_changed?
    ) && self.is_output_record? && self.resource_type == Resource_Types[:transform]
    # Note, always return true. This handler is not a
    # commit filter. It's just setting a boolean for
    # reference in the after_commit method.
    return true
  end

  def identify_data_maps
    CodeUtils.identify_data_maps(self) if self.saved_changes.keys.include?("code")
  end

  def after_commit
    TransformService.new.update_custom_script(self) if self.is_output_attribute?
    if (self.emit_update_messages)
      self.data_sets.each do |ds|
        ControlService.new(ds).publish(:update) if ds.active?
      end
    end
    return true
  end

end
