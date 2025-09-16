module Api::V1
  class ApprovalRequestsController < ApiController
    # Make sure that pending status check is done in transaction
    around_action :wrap_in_transaction, only: [:reject, :approve, :cancel]

    before_action do
      # TODO: `orgs/_member` partial requires that to work
      @org_members, @org_access_roles, @org_roles_expirations = Org.org_users_with_roles(current_org)
    end

    def requested
      @collection = base_scope.where(requestor_id: current_user.id)
      render 'index'
    end

    def assigned
      @collection = base_scope.joins(:approval_steps).where(approval_steps: {status: :pending, assignee_id: current_user.id})
      render 'index'
    end

    def pending
      @collection = pending_scope.joins(:approval_steps).where(approval_steps: {status: :pending, assignee_id: nil})
      render 'index'
    end

    def show
      authorize! :read, approval_request

      render 'show'
    end

    def approve
      authorize! :manage, approval_request
      ensure_pending_status!

      current_action.perform!

      render 'show'
    end

    def reject
      authorize! :manage, approval_request
      ensure_pending_status!

      action = ApprovalSteps::Reject.new(performer: current_user, org: current_org, step: current_step, **request_parameters)
      action.perform!

      head :ok
    end

    def cancel
      ensure_pending_status!

      action = ::ApprovalRequests::Cancel.new(performer: current_user, org: current_org, approval_request: approval_request)
      action.perform!

      head :ok
    end

    def show_data_set
      @data_set = data_sets_scope.find(params[:data_set_id])
      render 'show_data_set'
    end

    protected

    def data_sets_scope
      ids = pending_scope.pending.map { |ar| ar.first_step&.result&.fetch(:data_set_id, nil) }.compact
      current_org.data_sets.where(id: ids)
    end

    def ensure_pending_status!
      raise Api::V1::ApiError.new(:conflict, "Approval Request already has #{approval_request.status}") unless approval_request.pending?
    end

    memoize def approval_request
      base_scope.find(params[:id])
    end

    memoize def current_action
      current_step.action_class.new(performer: current_user, org: current_org, step: current_step, outcome: :approved, **request_parameters)
    end

    memoize def request_parameters
      request.raw_post.present? ? validate_body_json(ApprovalRequest) : {}
    end

    delegate :current_step, to: :approval_request

    helper_method :approval_request

    def base_scope
      scope = current_org.approval_requests

      if params[:type].present?
        scope.where(request_type: params[:type])
      else
        scope
      end
    end

    def pending_scope
      # TODO: Change when added more types of ApprovalRequests
      if current_user.org_custodian?(current_org) || current_org.has_admin_access?(current_user)
        base_scope
      elsif current_user.domain_custodian?
        case params[:type].to_sym
        when :marketplace_item_access
          base_scope.where(topic_id: current_user.custodian_of_marketplace_item_ids)
        when :marketplace_item
          base_scope.where(topic_id: current_user.custodian_for_domain_ids)
        else
          raise Api::V1::ApiError.new(:forbidden)
        end
      else
        raise Api::V1::ApiError.new(:forbidden)
      end
    end

    def wrap_in_transaction
      ApprovalRequest.transaction do
        approval_request.lock!

        yield
      end
    end
  end
end
