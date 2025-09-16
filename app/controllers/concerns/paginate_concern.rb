module PaginateConcern
  extend ActiveSupport::Concern

  class_methods do
    def paginated_actions
      @paginated_actions ||= Hash.new
    end

    # Sets pagination for given endpoint
    # @param action Symbol Name of the action to be paginated
    # @param per_page Integer maximum pagination allowed
    # @param enforce Boolean automatically set pagination for all requests
    def pagination(action, per_page: 50, enforce: true)
      paginated_actions[action] = { per_page: per_page, enforce: enforce }
    end
  end

  private

  # See NEX-12792. Currently, if no &per_page query paramter is passed by
  # caller, @per_page defaults to PAGINATE_ALL_COUNT (really big number).
  # Here we cap it to a number that performs reasonably well in Nexla prod.
  # Note, if the caller didn't pass any pagination params, we have to set 
  # @paginate == true to emit pagination headers in set_link_header().
  def enforce_pagination_for_endpoint
    return unless paginate_enforcer_set?

    if enforce_pagination?
      fail_with(:bad_request, "per_page is greater than maximum #{current_pagination_limit} allowed for that endpoint") if per_page > current_pagination_limit
    else
      @per_page = current_pagination_limit if per_page > current_pagination_limit
    end

    @paginate = true
  end

  def paginate_enforcer_set?
    current_pagination_limit.present?
  end

  def current_pagination_limit
    self.class.paginated_actions.dig(action_name.to_sym, :per_page)
  end

  def enforce_pagination?
    @paginate || self.class.paginated_actions.dig(action_name, :enforce)
  end
end
