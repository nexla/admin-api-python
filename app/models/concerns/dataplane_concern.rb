module DataplaneConcern
  extend ActiveSupport::Concern

  included do |base|
    if base.has_attribute?(:cluster_id)
      base.scope :in_dataplane, -> (dataplane) {
        where(cluster_id: dataplane.id)
      }
    elsif base.has_attribute?(:org_id)
      base.scope :in_dataplane, -> (dataplane) { 
        where(org_id: Org.where(cluster_id: dataplane.id).pluck(:id))
      }
    end
  rescue ActiveRecord::StatementInvalid => e
    raise e unless Rails.env.test? || ENV["DB_INIT"].truthy? # DB initialization problem
  end
end
