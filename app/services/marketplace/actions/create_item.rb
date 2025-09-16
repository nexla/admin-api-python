module Marketplace
  module Actions
    class CreateItem
      def initialize(domain, item, api_user_info)
        @domain = domain
        @item = item.symbolize_keys
        @api_user_info = api_user_info
      end

      def call
        Domain.transaction do
          validate!
          step = ApprovalRequests::Create.new(performer: api_user_info.user, type: 'marketplace_item', topic: domain, org: domain.org).perform!
          item_attributes = @item.slice(*step.action_class.attribute_names.map(&:to_sym))
          step.action_class.new(performer: api_user_info.user, org: domain.org, step: step, outcome: :approved, **item_attributes).perform!
          step.approval_request
        end
      end

      protected

      attr_reader :item, :domain, :api_user_info

      def validate!
        raise Api::V1::ApiError.new(:bad_request, "Data set is not provided") unless item[:data_set_id].present?
        data_set = DataSet.find(item[:data_set_id])
        raise Api::V1::ApiError.new(:not_found, "Data set could not be found") unless data_set.present?
        raise Api::V1::ApiError.new(:forbidden, "Data set doesn't belong to domain's org") if data_set.org_id != domain.org_id

        raise Api::V1::ApiError.new(:conflict, "Data set is already added to the Domain") if domain.data_set_ids.include?(data_set.id)
        raise Api::V1::ApiError.new(:conflict, "Data set is pending approval to the Domain") if domain.requested_marketplace_items_ids.include?(data_set.id)

        raise Api::V1::ApiError.new(:forbidden, "Data set is not available for org") unless data_set.org == api_user_info.org
        raise Api::V1::ApiError.new(:forbidden, "Data set is not accessible for user") unless data_set.has_collaborator_access?(api_user_info.user)
      end
    end
  end
end
