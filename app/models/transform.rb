class Transform < CodeContainer
  self.table_name = "code_containers"
  include AccessControls::Standard
  include Accessible


  TRANSFORM_RESOURCE_TYPES = [CodeContainer::Resource_Types[:transform], CodeContainer::Resource_Types[:splitter]].freeze

  scope :reusable, -> { where(
      :reusable => true,
      :output_type => CodeContainer::Output_Types[:record],
      :resource_type => CodeContainer::Resource_Types[:transform]
    )
  }

  scope :find_public, -> { where(
      :public => true,
      :output_type => CodeContainer::Output_Types[:record],
      :resource_type => CodeContainer::Resource_Types[:transform]
    )
  }

  has_many :data_sets, foreign_key: :code_container_id

  Jolt_Spec_Ops = {
    :custom => "nexla.custom"
  }

  def self.find (transform_id)
    tx = Transform.where(:id => transform_id,
      :resource_type => TRANSFORM_RESOURCE_TYPES,
      :output_type => CodeContainer::Output_Types[:record]).first
    raise ActiveRecord::RecordNotFound.new("Couldn't find Transform with 'id'=#{transform_id}") if tx.nil?
    return tx
  end

  def self.first
    return Transform.where(:resource_type => TRANSFORM_RESOURCE_TYPES,
      :output_type => CodeContainer::Output_Types[:record]).first
  end

  def self.all
    super.where(:resource_type => TRANSFORM_RESOURCE_TYPES,
      :output_type => CodeContainer::Output_Types[:record])
  end

  def self.empty_api_wrapper_transform
    return {
      "version" => 1,
      "data_maps" => [],
      "transforms" => [],
      "custom_config" => {}
    }
  end

  def self.api_wrapper_format? (tx)
    (tx.is_a?(Hash) && (tx.key?("transforms") || tx.key?(:transforms)))
  end

  def self.jolt_spec_format? (tx)
    return true if tx.is_a?(Array)
    (tx.is_a?(Hash) && (tx.key?("operation") || tx.key?(:operation)))
  end

  def self.build_from_input (api_user_info, input)
    if (input.blank? || api_user_info.input_owner.nil?)
      raise Api::V1::ApiError.new(:bad_request, "Transform input missing")
    end
    input[:output_type] = CodeContainer::Output_Types[:record]
    return CodeContainer.build_from_input(api_user_info, input)
  end

  def get_jolt_spec (data_set = nil)
    if (self.is_output_attribute?)
      raise Api::V1::ApiError.new(:bad_request, "Invalid transform code") 
    end

    tx_code = self.get_code
    tx_code = [] if (tx_code.empty?)

    if (self.is_script? && tx_code.is_a?(String))
      tx_code = tx_code.gsub("\n", "") if (self.is_base64?)
      tx_code = [{
        "operation" => Jolt_Spec_Ops[:custom],
        "spec" => {
          "language" => self.code_type,
          "encoding" => self.code_encoding,
          "script" => tx_code
        }
      }]
    elsif (!tx_code.is_a?(Array))
      tx_code = [tx_code]
    end

    if (!data_set.nil? && !data_set.output_validator.nil?)
      tx_code << data_set.output_validator.get_jolt_operation
    end

    out_tx = {
      "version" => 1,
      "data_maps" => [],
      "transforms" => tx_code,
      "custom_config" => self.custom_config
    }

    out_tx["custom"] = true if self.is_jolt_custom?
    return out_tx
  end

  def initialize (*args)
    # Note, calling compact() here because FactoryBot 6.1
    # introduced some weird behavior when creating instances
    # of sub-classes, resulting in [nil] being received here
    # instead of [] for *args.
    super(args.compact)
    self.resource_type = CodeContainer::Resource_Types[:transform]
    self.output_type = CodeContainer::Output_Types[:record]
  end

end