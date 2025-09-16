class Invite < ApplicationRecord
  belongs_to :created_by_user, class_name: 'User'
  belongs_to :org

  before_save :ensure_uid

  def ensure_uid
    return if self.uid.present?

    uid = nil
    until uid && !Invite.exists?(uid: uid)
      uid = SecureRandom.hex(12)
    end
    self.uid = uid
  end
end
