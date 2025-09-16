class CodeFilter < ApplicationRecord
  self.primary_key = :id

  include AuditLog

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org

  Scopes = {
    :all => "all",
    :org => "org"
  }

  Code_Types = API_CODE_TYPES.merge({
    :config => "config",
    :all => "all"
  })

  def self.code_types_enum
    enum = "ENUM("
    first = true
    Code_Types.each do |k, v|
      enum += "," if !first
      enum += "'#{v}'"
      first = false
    end
    enum + ")"
  end

  def self.scopes_enum
    enum = "ENUM("
    first = true
    Scopes.each do |k, v|
      enum += "," if !first
      enum += "'#{v}'"
      first = false
    end
    enum + ")"
  end

  def regex
    Regexp.new(self.pattern)
  end

  def apply (code_str)
    return { :description => "Invalid code format" } if (!code_str.is_a?(String))
    m = code_str.scan(self.regex)
    return (m.blank? ? nil : { :description => self.description })
  end

end
