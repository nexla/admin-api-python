class DataSetAuditsWorker
  include Sidekiq::Worker

  MAX_COUNT = 30
  OBJECT_CHANGES = ["output_schema", "updated_at"]

  sidekiq_options queue: 'data_sets'

  def perform data_set_id
    begin
      retries ||= 0
      audits = DataSetVersion.where("item_id = ? and object_changes like '%#{OBJECT_CHANGES[0]}%'", data_set_id).order(created_at: :desc)
      count = 0
      delete_ids = []
      audits.each do |audit|
        if audit.object_changes.keys == OBJECT_CHANGES
          (count < MAX_COUNT) ? (count = count + 1) : delete_ids.push(audit.id) 
        end
      end
      DataSetVersion.where(id: delete_ids).delete_all
    rescue => e
      retry if (retries += 1) < 3
    end
  end
end
      