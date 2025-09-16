class NumericParams
  def initialize(input_hash)
    @input_hash = input_hash
  end

  attr_reader :input_hash

  def to_h
    transform(input_hash)
  end

  private

  def transform(value)
    case value
    when String
      int = value.to_i

      if int.to_s == value
        int
      else
        value
      end
    when Array then value.map { |v| transform(v) }
    when Hash then value.transform_values { |v| transform(v) }
    else
      value
    end
  end
end
