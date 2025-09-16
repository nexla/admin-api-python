module Api
  module V1
    class SearchHealthController < Api::V1::ApiController

      OK_RESPONSE = { result: true }.freeze
      FAIL_RESPONSE = { result: false, message: "Can't reach ES cluster" }.freeze

      before_action :require_nexla_admin!

      def show
        return render json: OK_RESPONSE if SearchService::BaseSearch.ping

        render json: FAIL_RESPONSE.merge( config: SearchService::BaseSearch.display_config )
      end

      def test
        render json: SearchService::BaseSearch.test_connection
      end

    end
  end
end
