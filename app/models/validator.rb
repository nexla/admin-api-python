class Validator < CodeContainer
  self.table_name = "code_containers"
  include AccessControls::Standard
  include Accessible

  scope :find_public, -> { where(
      :public => true,
      :output_type => CodeContainer::Output_Types[:record],
      :resource_type => CodeContainer::Resource_Types[:validator]
    )
  }
  
  has_many :data_sets, foreign_key: :output_validator_id

  Jolt_Spec_Op = {
    :operation => "nexla.schema.validator"
  }

  def self.find (validator_id)
    va = Validator.where(:id => validator_id,
      :resource_type => CodeContainer::Resource_Types[:validator],
      :output_type => CodeContainer::Output_Types[:record]).first
    raise ActiveRecord::RecordNotFound.new("Couldn't find Validator with 'id'=#{validator_id}") if va.nil?
    return va
  end

  def self.first
    return Validator.where(:resource_type => CodeContainer::Resource_Types[:validator],
      :output_type => CodeContainer::Output_Types[:record]).first
  end

  def self.all
    super.where(:resource_type => CodeContainer::Resource_Types[:validator],
      :output_type => CodeContainer::Output_Types[:record])
  end

  def self.build_from_input (api_user_info, input)
    if (input.blank? || api_user_info.input_owner.nil?)
      raise Api::V1::ApiError.new(:bad_request, "Validator input missing")
    end
    input[:resource_type] = CodeContainer::Resource_Types[:validator]
    input[:output_type] = CodeContainer::Output_Types[:record]
    input[:reusable] = true
    return CodeContainer.build_from_input(api_user_info, input)
  end

  def initialize (*args)
    super(args.compact)
    self.resource_type = CodeContainer::Resource_Types[:validator]
    self.output_type = CodeContainer::Output_Types[:record]
    self.reusable = true
  end

  def get_jolt_operation
    op = Jolt_Spec_Op.dup
    op[:spec] = Hash.new
    op[:spec][:script] = self.get_code
    op[:spec][:language] = self.code_type
    op[:spec][:encoding] = self.code_encoding
    return op
  end

end