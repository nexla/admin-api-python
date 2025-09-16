class DataSetNotifySharersWorker
  include Sidekiq::Worker

  sidekiq_options queue: 'data_sets', retry: 3

  def perform(data_set_id)
    data_set = DataSet.find_by_id(data_set_id)
    NotificationService.new.publish_share_dataset(data_set) if data_set.present?
  end
end
      