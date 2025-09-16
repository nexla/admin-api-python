class ProjectsDataFlow < ApplicationRecord
  belongs_to :data_source
  belongs_to :data_set
  belongs_to :data_sink
  belongs_to :project

  def resource
    return self.data_source if !self.data_source_id.nil?
    return self.data_sink if !self.data_sink_id.nil?
    return self.data_set
  end

  def resource_key
    return :data_source_id if !self.data_source_id.nil?
    return :data_sink_id if !self.data_sink_id.nil?
    return :data_set_id
  end

  def data_flow
    DataFlow.new({
      self.resource_key => self.resource.id,
      :user => self.resource.owner,
      :org => self.resource.org
    })
  end
end
