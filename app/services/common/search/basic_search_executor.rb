module Common
  module Search
    class BasicSearchExecutor
      def initialize(user, org, model, filters, scope = model.all, include_public: false, sort_opts: {})
        @filters = filters.dup
        @org = org
        @user = user
        @model = model
        @scope = scope
        @include_public = include_public
        @sort_opts = sort_opts
      end

      def call
        return scope if !scope.nil? && scope.size.zero?
        return scope if filters.blank?

        non_public = scope.where(id: ids)
        return apply_order(non_public) unless include_public

        public = model.where(public: true, id: public_ids)
        apply_order(non_public.or(public))
      end

      def ids
        return [] if filters.blank?

        filters = add_users_filter(self.filters)
        hits = SearchWithElastic.new(user: user, model: model, filters: filters, org: org).call
        hits.map { |row| row['_source']['id'] }
      end

      def public_ids
        public_hits = SearchWithElastic.new(user: user, model: model, filters: filters, search_service_instance: SearchService::PublicSearch.new).call
        public_ids = public_hits.map { |row| row['_source']['id'] }
        public_ids
      end

      protected
      attr_reader :user, :org, :model, :filters, :scope, :include_public, :sort_opts

      # Note: this method doesn't modify `self.filters`, it returns a new array, which is used to filter by users and reduce the number of results.
      def add_users_filter(filters)
        return filters unless scope

        user_ids = scope.pluck(Arel.sql('distinct (owner_id)'))
        condition = { field: :owner_id, value: user_ids, operator: 'eq' }

        filters = self.filters.dup
        includes_or = filters.is_a?(Array) && filters.map{|v| v.is_a?(String) }.include?('or')
        if filters.is_a?(Array) && !includes_or
          filters << condition
        else
          filters = [ condition, filters ]
        end

        filters
      end

      def apply_order(scope)
        sort_field = sort_opts[:sort_by].try(:downcase) || 'created_at'
        sort_order = sort_opts[:sort_order].try(:downcase) || 'desc'

        unless model.column_names.include?(sort_field)
          raise ArgumentError.new("Invalid sort field: #{sort_field}")
        end

        scope.order(sort_field => sort_order)
      end
    end
  end
end