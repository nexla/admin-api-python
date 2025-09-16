class ExternalSharer < ApplicationRecord
  self.primary_key = :id

  belongs_to :data_set

  def render
    return {
      :email => self.email,
      :notified_at => self.notified_at,
      :name => self.name,
      :description => self.description
    }
  end

end
