module SearchableConcern
  extend ActiveSupport::Concern

  included do |base|

    base.include(SearchableInstanceMethods)
    base.extend(SearchableClassMethods)

    if base.name == 'Org'
      # When Org is created - create the index
      base.after_commit on: :create do |r|
        IndexingWorker::CreateIndex.perform_async r.id
      end
    elsif base.name == 'ActsAsTaggableOn::Tagging'
      # When Tag is updated - get taggable model and update it's data
      base.after_create_commit {|r|  call_tagging_indexing(r) }
      base.after_destroy {|r|  call_tagging_indexing(r) }
    else
      # Else - just update resource data
      base.after_create_commit {|r| call_resource_indexing(:create, r, base.name) }
      base.after_update_commit {|r| call_resource_indexing(:update, r, base.name)}
      base.after_destroy {|r| call_resource_indexing(:destroy, r, base.name)}
    end

    base.send(:alias_method, :searchable_attributes, :attributes) unless base.instance_methods.include?(:searchable_attributes)

    unless base.instance_methods.include?(:should_index?)
      base.send(:define_method, :should_index?) do
        true
      end
    end
  end

  module SearchableInstanceMethods
    def call_tagging_indexing(tag_model)
      with_error_logging(tag_model) do
        model = tag_model.taggable_type.constantize
        if SearchService::BaseSearch.supported_resources.include? model
          res = model.find(tag_model.taggable_id)
          IndexingWorker::IndexResource.perform_async :update.to_s, res.class.name, res.id, res.org_id
        end
      end
    end

    def call_resource_indexing(operation, resource, klass_name)
      with_error_logging(resource) do
        IndexingWorker::IndexResource.perform_async operation.to_s, klass_name, resource.id, resource.org_id
      end
    end

    def with_error_logging(resource)
      begin
        yield
      rescue => e
        logger = Rails.configuration.x.error_logger
        logger.error({
                       event: "indexing the resource",
                       class: "IndexingWorker",
                       id: resource.id,
                       resource: resource.class.name,
                       error: e.message
                     }.to_json)
      end
    end

    def wrap_vendor_part(vendor_part)
      "::#{vendor_part}::"
    end

    def wrap_vendor_parts(parts)
      parts.compact.map {|p| wrap_vendor_part(p) }.join(' ').presence
    end
  end

  module SearchableClassMethods
    def searchable_attributes_names
      @searchable_attributes_names ||= self.new.searchable_attributes.keys.map(&:to_s)
    end
  end

end
