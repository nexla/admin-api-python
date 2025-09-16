module Api::V1
  class SelfSignupBlockedDomainsController < Api::V1::ApiController
    Default_Per_Page = PAGINATE_ALL_COUNT

    before_action :require_nexla_admin!

    def index
      @self_signup_blocked_domains = SelfSignupBlockedDomain.all.paginate(page: params[:page], per_page: params[:per_page] || Default_Per_Page)
    end

    def create
      domain = params[:domain]
      if domain.present?
        SelfSignupBlockedDomain.create(domain: domain)
        render action: :index
      else
        render json: { error: 'Domain is required' }, status: :unprocessable_entity
      end
    end

    def update
      if self.params[:domain].blank?
        return render json: { error: 'Failed to update domain' }, status: :unprocessable_entity
      end

      self_singup_blocked_domain = SelfSignupBlockedDomain.find(params[:id])
      self_singup_blocked_domain.update(domain: params[:domain])
      render action: :index

    end

    def destroy
      self_singup_blocked_domain = SelfSignupBlockedDomain.find(params[:id])
      if self_singup_blocked_domain.destroy
        render action: :index
      else
        render json: { error: 'Failed to delete domain' }, status: :unprocessable_entity
      end
    end
  end
end