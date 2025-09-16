json.(@user, :id, :email, :full_name, :email_verified_at, :updated_at, :created_at)
json.partial!(@api_root + "users/login_history", audits: @login_history) if @login_history.present?
json.partial!(@api_root + "users/logout_history", audits: @logout_history) if @logout_history.present?