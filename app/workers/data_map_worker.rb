class DataMapWorker
  include Sidekiq::Worker

  sidekiq_options queue: 'data_maps'

  def perform (data_map_id, action)
    data_map = DataMap.find(data_map_id)
    case action.to_sym
    when :refresh
      TransformService.new.refresh_data_map(data_map)
    end
  rescue Exception => e
    Rails.configuration.x.transform_service_logger.error("EXCEPTION: #{e.message}, #{e.backtrace[0..5]}")
  end
end
