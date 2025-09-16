class EndpointSpec < ApplicationRecord
  include AuditLog

  belongs_to :org, required: true
  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :data_set, required: true

  after_save :manage_mapping
  after_destroy :destroy_mapping

  def self.endpoint_methods_enum
    "ENUM(" + API_ENDPOINT_METHODS.values.map{|v| "'#{v}'"}.join(",") + ")"
  end

  def self.default_endpoint_method
    API_ENDPOINT_METHODS[:get]
  end

  def self.build_from_input(api_user_info, input)
    return nil if api_user_info.blank?
    return nil unless input.is_a?(Hash)

    input.symbolize_keys!

    raise Api::V1::ApiError.new(:bad_request, "Invalid data set") if input[:data_set_id].blank?

    data_set = DataSet.find(input[:data_set_id])
    input.delete(:data_set_id)

    ability = Ability.new(api_user_info.input_owner)
    raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data set") unless ability.can?(:manage, data_set)

    spec = EndpointSpec.new
    spec.data_set = data_set
    spec.set_defaults(api_user_info)
    spec.update_mutable!(api_user_info, input)

    return spec
  end

  def set_defaults(api_user_info)
    self.owner = api_user_info.input_owner
    self.org = api_user_info.input_org
  end

  def update_mutable!(api_user_info, input)
    return nil if api_user_info.blank?
    return nil unless input.is_a?(Hash)

    input.symbolize_keys!

    self.org = api_user_info.input_org if (self.org != api_user_info.input_org)
    self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)

    self.method = input[:method] if input.key?(:method) && input[:method].present?
    self.headers = input[:headers] if input.key?(:headers)
    self.path_params = input[:path_params] if input.key?(:path_params)
    self.query_params = input[:query_params] if input.key?(:query_params)
    self.body = input[:body] if input.key?(:body)

    self.route = build_route

    self.save!
  end

  def sample
    {
      headers: self.headers&.map { |h| [h["key"], h["value"]] }.to_h || {},
      path_params: self.path_params&.filter_map { |p| [p["key"].gsub(/[{}]/, ''), p["value"]] if p.key?("value") }.to_h || {},
      query_params: self.query_params&.map { |q| [q["key"], q["value"]] }.to_h || {},
      body: self.body || {}
    }
  end

  private

  def build_route
    return '/' if self.path_params.blank?

    paths = []
    if self.path_params.is_a?(Hash)
      paths << self.path_params['key']
    elsif self.path_params.is_a?(Array)
      paths << self.path_params.pluck('key')
    end

    '/' + paths.join('/')
  end

  def manage_mapping
    attrs = ["method", "route"]
    return unless previously_new_record? || (saved_changes.keys & attrs).any?

    source = self.data_set&.data_source
    raise Api::V1::ApiError.new(:bad_request, "Invalid data source") unless source.present?

    mapping = source.endpoint_mappings.find_or_create_by(data_set_id: self.data_set_id) do |m|
      m.name = self.data_set.name,
      m.description = self.data_set.description
    end
    mapping.update!(
      { name: self.data_set.name, description: self.data_set.description}.merge(self.attributes.slice(*attrs))
    )
  end

  def destroy_mapping
    source = self.data_set&.data_source
    source&.endpoint_mappings&.where(data_set_id: self.data_set_id)&.destroy_all
  end
end
