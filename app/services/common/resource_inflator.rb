module Common::ResourceInflator
  class << self

    def singularize_resource_name(resource_name)
      resource_name = resource_name.singularize if resource_name&.underscore != 'data_credentials'
      resource_name
    end

    def singularize_resource_name_abs(resource_name)
      resource_name = resource_name.singularize
      resource_name
    end

    def class_name_by_resource_name(resource_name)
      singularize_resource_name(resource_name).camelize
    end

    def class_by_resource_name(resource_name)
      class_name = class_name_by_resource_name(resource_name)
      "::#{class_name}".constantize
    end

    def reference_field_name(resource_name)
      resource_name = singularize_resource_name(resource_name.underscore)
      "#{resource_name}_id"
    end

    def id_param_name(resource_name)
      field = singularize_resource_name_abs(resource_name)
      "#{field.underscore}_id"
    end

    def association_name(resource_name)
      resource_name.underscore.pluralize
    end

  end
end