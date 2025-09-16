class DataSchema < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AccessControls::Standard
  include JsonAccessor
  include AuditLog
  include Copy
  include Docs
  include SearchableConcern
  include Accessible
  include DataplaneConcern

  belongs_to :owner, class_name: "User", foreign_key: "owner_id"
  belongs_to :org
  belongs_to :data_credentials
  belongs_to :copied_from, class_name: "DataSchema", foreign_key: "copied_from_id"

  json_accessor :schema, :annotations, :validations
  json_array_accessor :data_samples, filtered: true

  scope :public_scope, -> { where(public: true) }
  
  acts_as_taggable_on :tags
  def tags_list
    self.tags.pluck(:name)
  end
  alias_method :tag_list, :tags_list

  def self.build_from_input (api_user_info, input)
    input.symbolize_keys!
    template = !!input[:template]

    tags = input.delete(:tags)

    if (!input[:data_schema_id].nil?)
      ds = DataSchema.find(input[:data_schema_id])
      if (!Ability.new(api_user_info.input_owner).can?(:read, ds))
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data schema")
      end
      return DataSchema.clone_from_data_schema(ds, template)
    end

    if (!input[:data_set_id].nil?)
      ds = DataSet.find(input[:data_set_id])
      if (!Ability.new(api_user_info.input_owner).can?(:read, ds))
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data set")
      end
      return DataSchema.clone_from_data_set(ds, template)
    end

    input[:owner] = api_user_info.input_owner
    input[:org] = api_user_info.input_org

    schema =  DataSchema.create(input)
    ResourceTagging.add_owned_tags(schema, {tags: tags}, api_user_info.input_owner)
    schema
  end

  def self.clone_from_data_set (ds, template = false)
    return DataSchema.create({
      :owner => ds.owner,
      :org => ds.org,
      :name => "Copied Schema" + (ds.name.blank? ? "" : ds.name),
      :description => "Data schema copied from data set #{ds.id}",
      :template => template,
      :schema => ds.output_schema,
      :annotations => ds.output_schema_annotations,
      :validations => ds.output_validation_schema,
      :data_samples => ds.data_samples
    })
  end

  def data_sets
    # NEX-11372: the out_schema_id and src_schema_id attributes that
    # this method formerly selected on were never actually used (or
    # exposed in API responses). The query, however, became extremely
    # slow due to 1) lack of indexes, and 2) lack of org scoping.
    # For backwards-compatibilty we're going to continue returning what 
    # this method always returned: an empty ActiveRecord relation. We
    # might eventually define/implement a meaningful response for this.
    DataSet.none
  end

  def latest_version
    # TODO: In Rails 6 replace `limit(1).pluck(:id).first` with `pick(:id)`
    DataSchemaVersion.where(:item_id => self.id).order(created_at: :desc).limit(1).pluck(:id).first.presence || 1
  end

  def update_mutable! (api_user_info, input, request)
    return if (input.blank? || api_user_info.nil?)
    if (!!input[:template] && !self.data_sets.empty?)
      raise Api::V1::ApiError.new(:bad_request,
        "Data schema associated with a data set cannot be marked as a template (#{self.data_sets.first.id}). Copy it first.")
    end

    if (input.key?(:public))
      raise Api::V1::ApiError.new(:method_not_allowed, "Input cannot include public attribute") if !api_user_info.user.super_user?
      self.public = !!input[:public]
    end

    tags = input.delete(:tags)

    self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
    self.org = api_user_info.input_org if (self.org != api_user_info.input_org)
    self.name = input[:name] if !input[:name].blank?
    self.description = input[:description] if input.key?(:description)
    self.detected = input[:detected] if input.key?(:detected)
    self.managed = input[:managed] if input.key?(:managed)
    self.template = input[:template] if input.key?(:template)
    self.schema = input[:schema] if input.key?(:schema)
    self.annotations = input[:annotations] if input.key?(:annotations)
    self.validations = input[:validations] if input.key?(:validations)
    self.data_samples = input[:data_samples] if input.key?(:data_samples)
    self.save!

    ResourceTagging.add_owned_tags(self, {tags: tags}, api_user_info.input_owner)
  end

  def destroy
    if (!self.data_sets.empty?)
      ids = self.data_sets.map(&:id)
      raise Api::V1::ApiError.new(:method_not_allowed, "data_schema in use by one or more data sets: #{ids.inspect}")
    end
    super
  end
 end
