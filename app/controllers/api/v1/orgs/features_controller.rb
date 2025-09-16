module Api::V1::Orgs
  class FeaturesController < Api::V1::ApiController
    before_action do
      unless current_user.super_user?
        raise Api::V1::ApiError.new(:forbidden)
      end
    end

    rescue_from 'ArgumentError' do
      raise Api::V1::ApiError.new(:not_found, 'Feature not found')
    end

    def enable
      org_context.enable_feature!(feature_name)
    end

    def disable
      org_context.disable_feature!(feature_name)
    end

    private

    memoize def org_context
      Org.find(params[:org_id])
    end

    def feature_name
      params[:feature_name].to_s
    end
  end
end
