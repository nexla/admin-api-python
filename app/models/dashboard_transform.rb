class DashboardTransform < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AccessControls::Standard
  include JsonAccessor

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :code_container

  def self.build_from_input (input, user, org = nil)
    return nil if !input.is_a?(Hash)
    input.symbolize_keys!

    input[:owner_id] = user.id
    input[:org_id] = org.nil? ? nil : org.id

    transform = DashboardTransform.new

    transform.set_defaults(user, org)

    if input.key?(:resource_type) and input.key?(:resource_id)
      resource_type = QuarantineSetting.validate_resource_type_str(input[:resource_type])
      raise Api::V1::ApiError.new(:bad_request, "Unknown Resource type") if resource_type.nil?

      transform.resource_type = input[:resource_type]
      transform.resource_id = input[:resource_id]
    else
      transform.resource_type = 'USER'
      transform.resource_id = user.id
    end

    transform.update_mutable!({}, user, org, input)

    return transform
  end

  def update_mutable! (request, user, org, input)
    return if input.nil?
    ability = Ability.new(user)

    if (input.key?(:transform_id))
      code_container = CodeContainer.find(input[:transform_id])
      if (!ability.can?(:manage, code_container))
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to code container")
      end
      if (!code_container.reusable?)
        raise Api::V1::ApiError.new(:bad_request, "Cannot reuse that code container")
      end
      self.code_container_id = input[:transform_id]
    elsif (input.key?(:transform))
      input[:transform].symbolize_keys!

      code_config = input[:transform][:code_config] || {}
      code = input[:transform][:transforms] || {}
      raise Api::V1::ApiError.new(:bad_request, "Code container missing required code_config or code") if (code_config.empty? && code.empty?)

      FlowsDashboardService.new.validate_metrics_transform_function(code, org)

      name = input[:transform][:name] || "Error Transform"

      code_container = CodeContainer.new
      code_container.owner = user
      code_container.org = org
      code_container.code_config = code_config
      code_container.code = code
      code_container.name = name
      code_container.reusable = true
      code_container.output_type     = input[:transform][:output_type] if !input[:transform][:output_type].blank?
      code_container.code_encoding   = input[:transform][:code_encoding] if !input[:transform][:code_encoding].blank?
      code_container.resource_type   = CodeContainer::Resource_Types[:error]
      code_container.code_type       = input[:transform][:code_type] || CodeContainer::Code_Types[:python]
      code_container.save

      self.code_container_id = code_container.id
    end

    self.save!
  end

  def set_defaults (user, org)
    self.owner = user
    self.org = org
  end

end
