class TemplateVariableExtractor
  def initialize(template)
    @template = template.to_s
  end

  attr_reader :template

  def get
    result = []
    offset = 0
    loop do
      param, addition = get_next(template[offset..])
      break if param.nil?
      result << param
      offset += addition
    end
    result
  end

  private

  def get_next(template)
    front_offset = 0
    back_offset = 0
    from = 0
    to = 0
    result = nil

    loop do
      from = template.index('{', front_offset)
      return [nil, nil] unless from.present?

      next_char = template[from + 1] # Ignore JSON objects, as next_char would be most likely "
      unless [*('a'..'z'), *('A'..'Z')].include?(next_char)
        front_offset = from + 1
        next
      end
      
      to = template.index('}', [from, back_offset].max)
      return [nil, nil] unless to.present?

      if to - from == 1 # Skip empty `{}`
        front_offset = to
        next
      end

      result = template[from..to]
      break if result.count('{') == result.count('}')
      back_offset = to + 1
    end

    [result[1...-1], to + 1]
  end
end
