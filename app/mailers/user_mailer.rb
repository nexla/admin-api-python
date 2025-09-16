class UserMailer < ApplicationMailer

  def send_external_dataset_share_notification(data_set, external_user)
    email = "serverops@nexla.com,sundeep@nexla.com"
    subject = "Dataset #{data_set.id} shared with external user #{external_user} #{Rails.env}"
    @data_set = data_set
    @external_user = external_user
    mail(to: email, subject: subject)
  end
end