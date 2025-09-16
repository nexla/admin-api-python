module Api::V1
  class CubejsController < Api::V1::ApiController
    def query
      input = JSON.parse(request.raw_post)

      if input['query'].blank?
        raise Api::V1::ApiError.new(:bad_request, "query param should be provided in the request body")
      end

      org = Org.find_by_id(params[:org_id])
      if params.key?(:org_id) && !org
        raise Api::V1::ApiError.new(:not_found, "Org not found")
      end
      org ||= current_org

      user = User.find_by_id(params[:user_id])

      if params.key?(:user_id) && !user
        raise Api::V1::ApiError.new(:not_found, "User not found")
      end

      if params.key?(:org_id) && !org
        raise Api::V1::ApiError.new(:not_found, "Org not found")
      end

      authorize!(:read, org)
      authorize!(:read, user) if user

      result = CubejsService.new.query(input, user, org)
      render json: result
    end
  end
end