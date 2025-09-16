json.owner do
  if (user.nil?)
    json.nil!
  else
    json.(user, :id, :full_name, :email, :email_verified_at)
  end
end
