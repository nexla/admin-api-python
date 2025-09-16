class Api::V1::Marketplace::DomainsController < Api::V1::ApiController

  before_action :find_domain, only: [:show, :update, :destroy, :create_item]
  before_action :authorize_manage_domains, only: [:create, :update, :destroy]
  before_action :authorize_read_domains, only: [:index, :show, :items, :items_search, :create_item]
  before_action :require_org, only: [:create, :create_item, :update, :destroy]
  before_action :init_page_params, only: [:index, :items]

  # TODO: unify with FlowsController once it's pagination established
  Max_Per_Page = 100
  Default_Per_Page = 20

  def index
    @domains = current_org.domains.with_items_count.paginate(page: @page, per_page: @per_page)
    set_link_header(@domains)
  end

  def for_org_index
    if params[:org_id]
      org = Org.find(params[:org_id])
      authorize! :read, org
    else
      org = current_org
    end

    authorize_read_domains(org)

    @domains = org.domains.with_items_count.paginate(page: @page, per_page: @per_page)
    set_link_header(@domains)
    render :index
  end

  def show
    authorize! :read, @domain
  end

  def items
    authorize! :read, domain if domain.present?

    ids = (domain.presence || current_org).marketplace_items.active.pluck(:data_set_id)
    @items = add_request_filters(
      DataSet.where(id: ids), DataSet
    ).page(@page).per_page(@per_page)
    set_link_header(@items)
  end

  def item_show
    @item = (domain.presence || current_org).marketplace_items.find_by!(data_set_id: params[:item_id])
  end

  def items_search
    sort_opts = params.slice(:sort_by, :sort_order)
    ids = (domain.presence || current_org).marketplace_items.active.pluck(:data_set_id)
    scope = DataSet.where(id: ids)
    scope = Common::Search::BasicSearchExecutor.new(current_user, current_org, DataSet, params[:filters], scope, sort_opts: sort_opts).call
    @data_sets = scope.page(@page).per_page(@per_page)
    set_link_header(@data_sets)
  end

  def create
    input = validate_body_json(Domain)
    @domain = Domain.build_from_input(input, ApiUserInfo.new(current_user, current_org))
    @domain.save!
    render :show
  end

  def create_item
    # TODO: `orgs/_member` partial requires that to work
    @org_members, @org_access_roles = Org.org_users_with_roles(current_org)

    input = validate_body_json(MarketplaceItem)
    approval_request = ::Marketplace::Actions::CreateItem.new(@domain, input, ApiUserInfo.new(current_user, current_org)).call
    render partial: 'api/v1/approval_requests/show', locals: { approval_request: approval_request }
  end

  # TODO: Move it to separate controller
  def request_access
    # TODO: `orgs/_member` partial requires that to work
    @org_members, @org_access_roles = Org.org_users_with_roles(current_org)
    raise Api::V1::ApiError.new(:conflict, "User already have access to Nexset") if marketplace_item.data_set.has_sharer_access?(current_user, current_org)

    action = business_action(ApprovalRequests::Create, topic: marketplace_item, type: 'marketplace_item_access', unique: true)
    approval_step = action.perform!
    render partial: 'api/v1/approval_requests/show', locals: { approval_request: approval_step.approval_request }
  end

  def delist_item
    authorize! :manage, domain
    marketplace_item.delist!
    head :ok
  end

  def update
    authorize! :manage, @domain

    input = validate_body_json(Domain).symbolize_keys
    @domain.update_mutable!(input, ApiUserInfo.new(current_user, current_org, input, @domain))
    render :show
  end

  def custodians
    mode = params[:mode].to_sym
    domain = Domain.find(params[:domain_id])
    if mode == :list
      authorize! :read, domain
      @custodians = domain.domain_custodian_users
    else
      authorize! :manage, domain
      input = validate_body_json(CustodiansRequest).symbolize_keys
      @custodians = domain.update_custodians!(current_user, input[:custodians], mode)
    end
    set_link_header(@custodians)
    render :custodians
  end

  def destroy
    authorize! :manage, @domain
    @domain.destroy!
    render json: {}
  end

  protected

  memoize def data_set
    domain.data_sets.find(params[:item_id])
  end

  memoize def marketplace_item
    domain.marketplace_items.find_by(data_set_id: data_set.id)
  end

  memoize def domain
    Domain.find_by(id: params[:domain_id])
  end

  def find_domain
    @domain = Domain.find(params[:domain_id])
  end

  def require_org
    raise Api::V1::ApiError.new(:forbidden, "Only orgs can manage domains") unless current_org
  end

  def authorize_manage_domains
    unless FeatureToggle.enabled?(:marketplace) && current_org.marketplace_enabled?
      raise Api::V1::ApiError.new(:forbidden, "Marketplace is not available for org")
    end

    item = action_name == 'create' ? Domain : (@domain || current_org)
    authorize! :manage, item
  end

  def authorize_read_domains(org = current_org)
    unless FeatureToggle.enabled?(:marketplace) && org.present? && org.marketplace_enabled?
      raise Api::V1::ApiError.new(:forbidden, "Marketplace is not available for org")
    end
  end

  def init_page_params
    @per_page ||= Default_Per_Page
    @per_page = [Max_Per_Page, @per_page].min
  end
end
