module Api
  module V1
    class RuntimesController < Api::V1::ApiController
      include PaperTrailControllerInfo
      include AccessorsConcern

      def index
        @runtimes = current_org.runtimes
      end

      def show
        @runtime = current_org.runtimes.find(params[:id])
      end

      def create
        authorize! :manage, current_org

        input = validate_body_json(Runtime).symbolize_keys
        @runtime = Runtime.build_from_input(current_user, current_org, input)
        render :show
      end

      def update
        @runtime = Runtime.find(params[:id])
        authorize! :manage, @runtime

        input = validate_body_json(Runtime).symbolize_keys
        @runtime.update_mutable(current_user, input)
        render :show
      end

      def destroy
        @runtime = Runtime.find(params[:id])
        authorize! :manage, @runtime

        @runtime.destroy
        head :ok
      end

      def activate
        @runtime = Runtime.find(params[:id])
        authorize! :manage, @runtime

        if params[:activate].truthy?
          @runtime.activate!
        else
          @runtime.deactivate!
        end
        render :show
      end

    end
  end
end