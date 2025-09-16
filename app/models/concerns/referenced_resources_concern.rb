# Concern for models that are referenced by or referencing other models.
# References are stored in a separate model with the name 'ResourcesReference'.

# Checks for presence of references in other models before destroy for models that stating they are used
# by other models with 'mark_as_referenced_resource'

# To list models that are being referenced in a model, call 'referencing_resources' with the names of the referenced models.
# Defines:
#   - class attribute 'is_referenced_resource' that is set to true for models that are referenced by other models
#   - class attribute 'referencing_fields' that is set to an array of fields that contain references to other models
#   - method 'verify_ref_resources!' that verifies that referenced resources exist and are accessible by the user
#   - method 'update_referenced_resources' that updates references to other models
#   - methods "ref_#{resource_name}_ids" that return IDs of referenced resources
#   - scope "#{resource_name}_refs" that returns references to a specific resource type
# Adds delete hook that checks if the model is referenced by other models and raises an error if it is.
# Adds 'referenced_resources' association to the model that returns references to other models.
# Adds 'references' alias to 'referenced_resources' association.
module ReferencedResourcesConcern
  extend ActiveSupport::Concern

  mattr_accessor :referencing_models
  self.referencing_models = []

  included do
    before_destroy :check_if_referenced!
    class_attribute :is_referenced_resource
    class_attribute :_referencing_resources
    class_attribute :referencing_fields
    self.is_referenced_resource = false
  end

  class_methods do
    def referencing_resources( *resources )
      self._referencing_resources = resources

      unless ReferencedResourcesConcern.referencing_models.map(&:name).include?(self.name)
        ReferencedResourcesConcern.referencing_models << self
      end

      has_many :referenced_resources, class_name: 'ResourcesReference', as: :referencing
      alias_method :references, :referenced_resources

      self.referencing_fields = resources

      resources.each do |r|
        method = "ref_#{r.to_s.pluralize}_ids".to_sym
        scope = "#{r.to_s.pluralize}_refs".to_sym

        define_method(method) do
          self.references.send(scope).pluck(:referenced_id)
        end
      end

    end

    def mark_as_referenced_resource
      self.is_referenced_resource = true
    end
  end

  def referenced_resources_enabled?
    self.org&.referenced_resources_enabled.truthy?
  end

  def verify_ref_resources!(user, input)
    return if !input.is_a?(Hash)
    return if self.class._referencing_resources.blank?

    errors = []
    self.class._referencing_resources.each do |resource_name|
      resource_class = Common::ResourceInflator.class_by_resource_name(resource_name.to_s)
      key = resource_name.to_s.pluralize.to_sym
      resource_ids = input[key] || input[key.to_s]
      next if resource_ids.blank?

      resources = resource_class.where(id: resource_ids)
      not_found = resource_ids - (resources&.pluck(:id) || [])
      if not_found.present?
        errors << "Referenced #{resource_class.name} not found with ids: #{not_found}"
      end
      next if resources.blank?

      resources.each do |resource|
        is_public = resource.respond_to?(:public?) && resource.public?
        if ((resource.org_id != self.org_id) && !is_public) || !Ability.new(user).can?(:read, resource)
          errors << "Invalid access to referenced #{resource_class.name} with id: #{resource.id}"
        end
      end
    end

    if errors.present?
      status = if errors.all? { |error| error.include?('Referenced') }
                 :not_found
               elsif errors.all? { |error| error.include?('Invalid') }
                 :forbidden
               else
                 :bad_request
               end

      raise Api::V1::ApiError.new(status,
                                  "Referenced resources not found or not accessible.",
                                  nil,
                                  errors)
    else
      self.class._referencing_resources
    end
  end

  def update_referenced_resources(input)
    return if !input.is_a?(Hash)

    self.class._referencing_resources.each do |resource_name|
      next unless input.key?(resource_name) || input.key?(resource_name.to_s)

      klass_name = Common::ResourceInflator.class_name_by_resource_name(resource_name.to_s)
      resource_ids = input[resource_name] || input[resource_name.to_s]

      existing_refs = self.references.send("#{resource_name}_refs")
      existing_refs.where.not(referenced_id: resource_ids).destroy_all
      existing_ref_ids = existing_refs.where(referenced_id: resource_ids).pluck(:referenced_id)

      (resource_ids - existing_ref_ids).each do |resource_id|
        self.references.create!(org_id: self.org_id, referenced_id: resource_id, referenced_type: klass_name)
      end
    end
  end

  protected

  def check_if_referenced!
    if self._referencing_resources.present?
      ResourcesReference.where(referencing: self).destroy_all
    end

    if self.is_referenced_resource
      reference_type = Common::ResourceInflator.class_name_by_resource_name(self.class.name.to_s)

      reference = ResourcesReference.where(org_id: self.org_id, referenced_id: self.id, referenced_type: reference_type).first
      if reference
        resource = reference.referencing
        if resource # or else it's a deleted entity
          raise Api::V1::ApiError.new(:bad_request, "Cannot delete #{self.class.name} (ID=#{self.id}) because it is referenced by #{resource.class.name} (ID=#{resource.id}).")
        end
      end
    end
  end
end