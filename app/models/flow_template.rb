class FlowTemplate < ApplicationRecord
  belongs_to :owner, class_name: "User", foreign_key: "owner_id"
  belongs_to :org

  def validate!
    # TODO: Add validation to template using our schema checks like:
    # JSON::Validator.fully_validate(FlowTemplate.schema, self.template, :validate_schema => true)
    # Additional validations include checking for `flows` array not empty, checking `resources` array
    # exist and checking proper synthetic id format.
    #
    # This will be addressed in epic NEX-13048.
  end

  def self.load_from_config
    admin = User.find_by_email("admin@#{Org::Nexla_Admin_Email_Domain}")
    org = Org.get_nexla_admin_org
    return if org.blank? || admin.blank?

    return unless ActiveRecord::Base.connection.table_exists?(FlowTemplate.table_name)
    templates = Dir["#{Rails.root}/config/api/flow_templates/*.json"]
    templates.each do |template|
      spec = JSON.parse(File.read(template))
      next if FlowTemplate.find_by(flow_type: spec["flow_type"], default: true).present?

      spec["owner_id"] = admin.id
      spec["org_id"] = org.id
      FlowTemplate.create!(spec)
    end
  end
end
