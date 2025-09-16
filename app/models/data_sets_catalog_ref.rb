class DataSetsCatalogRef < ApplicationRecord
  include Api::V1::Schema
  include JsonAccessor
  include AuditLog

  belongs_to :data_set
  belongs_to :catalog_config

  Statuses = {
    :pending => "PENDING",
    :done => "DONE",
    :error => "ERROR"
  }

  scope :with_active_catalog_config, -> { joins(:catalog_config).where(catalog_configs: {status: "ACTIVE"}) }
  
  def resource
    self.data_set
  end

  def done?
    self.status == Statuses[:done]
  end

  def pending?
    self.status == Statuses[:pending]
  end

  def error?
    self.status == Statuses[:error]
  end

  def self.build_from_input(api_user_info, input)
    if (input.blank?)
      raise Api::V1::ApiError.new(:bad_request, "Required input missing")
    end
    data_ref = DataSetsCatalogRef.new
    data_ref.update_mutable!(api_user_info, input)
    return data_ref
  end

  def update_mutable! (api_user_info, input) 
    if (input.blank?)
      raise Api::V1::ApiError.new(:bad_request, "Required input missing")
    end
    # Check if dataset exists or not.
    if input.key?(:data_set_id)
      if DataSet.exists?(org_id: api_user_info.org.id, id: input[:data_set_id])
        self.data_set_id = input[:data_set_id]
      else
        raise Api::V1::ApiError.new(:not_found, "Not found")
      end
    end
    # Check if catalog config exists or not.
    if input.key?(:catalog_config_id)
      if CatalogConfig.exists?(org_id: api_user_info.org.id, id: input[:catalog_config_id])
        self.catalog_config_id = input[:catalog_config_id]
      else
        raise Api::V1::ApiError.new(:not_found, "Not found")
      end
    end
    # Check if status is valid or not.
    if input.key?(:status)
      if Statuses.values.include?(input[:status])
        self.status = input[:status]
      else
        raise Api::V1::ApiError.new(:bad_request, "Invalid status")
      end
    end
    self.reference_id = input[:reference_id] if input.key?(:reference_id)
    self.link = input[:link] if input.key?(:link)
    self.error_msg = input[:error_msg] if input.key?(:error_msg)
    self.save!
  end

  def self.bulk_update_refs(api_user_info, input)
    raise Api::V1::ApiError.new(:bad_request, "payload for bulk_update_refs should be an array") unless input.is_a?(Array)

    user = api_user_info.user
    ability = Ability.new(user)

    DataSetsCatalogRef.transaction do
      input.map do |input_item|
        catalog_ref = DataSetsCatalogRef.find_by(id: input_item[:id])
        raise Api::V1::ApiError.new(:not_found, "Catalog ref not found") unless catalog_ref

        unless ability.can?(:manage, catalog_ref)
          raise Api::V1::ApiError.new(:forbidden, "Not authorized to update catalog ref")
        end

        if input_item.key?(:status)
          status = input_item[:status].upcase
          if Statuses.values.include?(status)
            catalog_ref.status = status
          else
            raise Api::V1::ApiError.new(:bad_request, "Invalid status '#{status}''")
          end
        end

        catalog_ref.reference_id = input_item[:reference_id] if input_item.key?(:reference_id)
        catalog_ref.error_msg = input_item[:error_msg] if input_item.key?(:error_msg)
        catalog_ref.save!
        catalog_ref
      end
    end
  end
end
