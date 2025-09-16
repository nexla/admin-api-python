module Api
  module V1
    class AccessInsightsController < Api::V1::ApiController
      def explain_user_resource_access
        user = user_for_explain
        authorize! :manage, resource

        result = Access::Insights::Queries::ExplainResourceAccess.new(user, resource).call
        render json: result
      end

      def accessible_resources
        collection_name = Common::ResourceInflator.association_name(params[:resource_type]).to_sym

        begin
          if (klass_for_resource.respond_to?(:accessible_by_user))
            resources = current_user.send(collection_name, current_org, access_role: :all)
          else
            resources = current_user.send(collection_name, :all, current_org)
          end
        rescue NoMethodError
          raise Api::V1::ApiError.new(:bad_request, 'Wrong resource type')
        end

        if collection_name == :data_sets
          shared = DataSet.shared_with_user(current_user, current_org)
          resources = (resources.to_a + shared.to_a ).uniq
        end

        result = Access::Insights::Queries::
          ExplainMultipleResources.new(current_user, current_org, resources, params[:resource_type]).call

        render json: result
      end

      def explain_user_flow_access
        user = user_for_explain

        if params[:model] == 'Flow'
          origin_nodes = FlowNode.where(id: params[:id])
          authorize! :manage, origin_nodes.first

        elsif params[:model] == 'Project'
          project = Project.find(params[:id])
          authorize! :manage, project
          origin_nodes = project.flow_nodes

        else
          raise Api::V1::ApiError.new(:bad_request, 'Wrong entity type')
        end

        origin_nodes = origin_nodes.map(&:origin_node).compact

        result = Access::Insights::Queries::ExplainFlowAccess.new(user, current_org, origin_nodes).call
        render json: result
      end

      def users_access_insights
        target = if params[:resource_type] == 'flows'
                   FlowNode.find(params[:id])
                 else
                   resource
                 end
        authorize! :manage, target

        result = Access::Insights::Queries::UsersAccessToResourceInsights.new(target).call
        render json: result
      end

      private

      def resource
        @resource ||= begin
          param_name = Common::ResourceInflator.id_param_name(params[:resource_type])
          klass_for_resource.find(params[param_name])
        end
      end

      def klass_for_resource
        begin
          resource_type = params[:resource_type].to_s

          return FlowNode if resource_type == 'flows'

          Common::ResourceInflator.class_by_resource_name(resource_type)
        rescue StandardError => e
          raise Api::V1::ApiError.new(:bad_request, 'Wrong resource type')
        end
      end

      def user_for_explain
        if params[:accessor_user_id] || params[:user_id]
          user = User.find_by_id(params[:accessor_user_id] || params[:user_id])
          # Note, @api_user does not need :read or :manage
          # access to :accessor_user_id. As long as they have
          # proper access to the resource, they can examine
          # another user's access to it, with one caveat:
          # :accessor_user_id MUST also be a member of @api_org,
          # unless @api_user is a Nexla admin.
          raise Api::V1::ApiError.new(:not_found) if user.nil?
          if current_org.present?
            # Note, we must set the org context of the user_for_explain
            # result so that methods like resource.has_XYZ_access?
            # can be called. The answer always depends on the org
            # context in which the request is being handled.
            user.org = current_org
            
            if !user.org_member?(current_org) && !current_user.super_user?
              # Note, no explanation returned here. Better not
              # to help someone who is fishing on this endpoint.
              raise Api::V1::ApiError.new(:forbidden)
            end
          end
        else
          user = current_user
        end
        user
      end
    end

  end
end
