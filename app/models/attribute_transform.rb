class AttributeTransform < CodeContainer
  self.table_name = "code_containers"
  include AccessControls::Standard
  include Accessible

  scope :reusable, -> { where(
      :reusable => true,
      :output_type => CodeContainer::Output_Types[:attribute],
      :resource_type => CodeContainer::Resource_Types[:transform]
    )
  }

  scope :find_public, -> { where(
      :public => true,
      :output_type => CodeContainer::Output_Types[:attribute],
      :resource_type => CodeContainer::Resource_Types[:transform]
    )
  }

  def self.build_from_input (api_user_info, input)
    if (input.blank? || api_user_info.input_owner.nil?)
      raise Api::V1::ApiError.new(:bad_request, "Attribute transform input missing")
    end
    input[:output_type] = CodeContainer::Output_Types[:attribute]
    input[:reusable] = true if !input.key?(:reusable)
    return CodeContainer.build_from_input(api_user_info, input)
  end

  def self.find (attribute_transform_id)
    at = AttributeTransform.where(:id => attribute_transform_id,
      :resource_type => CodeContainer::Resource_Types[:transform],
      :output_type => CodeContainer::Output_Types[:attribute]).first
    raise ActiveRecord::RecordNotFound.new("Couldn't find AttributeTransform with 'id'=#{attribute_transform_id}") if at.nil?
    return at
  end

  def self.first
    return AttributeTransform.where(:resource_type => CodeContainer::Resource_Types[:transform],
      :output_type => CodeContainer::Output_Types[:attribute]).first
  end

  def self.all
    super.where(:resource_type => CodeContainer::Resource_Types[:transform],
      :output_type => CodeContainer::Output_Types[:attribute])
  end

  def initialize (*args)
    # Note, calling compact() here because FactoryBot 6.1
    # introduced some weird behavior when creating instances
    # of sub-classes, resulting in [nil] being received here
    # instead of [] for *args.
    super(args.compact)
    self.resource_type = CodeContainer::Resource_Types[:transform]
    self.output_type = CodeContainer::Output_Types[:attribute]
  end

end
