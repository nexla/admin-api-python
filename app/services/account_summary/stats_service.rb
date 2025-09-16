module AccountSummary
  class StatsService
    def initialize(org, user=nil)
      @org = org
      @user = user
    end

    def call
      result = {
        org_id: org.id,
        data_sources: calc_stats(DataSource),
        data_sinks: calc_stats(DataSink),
        data_sets: calc_data_sets_stats
      }
      result[:user_id] = user.id if user
      result
    end

    private

    attr_reader :user, :org

    def calc_data_sets_stats
      derived = calc_stats(DataSet.where.not(parent_data_set_id: nil))
      detected = calc_stats(DataSet.where(parent_data_set_id: nil))

      {
        derived: derived,
        detected: detected
      }
    end

    def calc_stats(scope, klass = nil)
      klass = scope.base_class if klass.nil? && scope.respond_to?(:base_class)

      statuses = klass::Statuses

      scope = scope.where(org: org)
      scope = scope.where(owner: user) if user
      by_status = scope.group(:status).count

      stats = { total: by_status.values.sum }
      statuses.each_with_object(stats) do |(status_sym, sql_status), result|
        result_key = status_sym == :init ? :initialized : status_sym
        result[result_key] = by_status[sql_status] || 0
      end
    end
  end
end
