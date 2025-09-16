require 'zlib'

class DocContainer < ApplicationRecord
  include Api::V1::Schema
  include AccessControls::Standard
  include JsonAccessor
  include AuditLog
  include Copy
  include SearchableConcern

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org
  belongs_to :data_credentials
  belongs_to :copied_from, class_name: "DocContainer", foreign_key: "copied_from_id"

  json_accessor :repo_config

  acts_as_taggable_on :tags
  def tags_list
    self.tags.pluck(:name)
  end
  alias_method :tag_list, :tags_list

  Doc_Types = API_DOC_TYPES
  Doc_Repo_Types = API_DOC_REPO_TYPES
  TEXT_ENCODING = "UTF-8"

  def self.doc_types_enum
    enum = "ENUM("
    first = true
    Doc_Types.each do |k, v|
      enum += "," if !first
      enum += "'#{v}'"
      first = false
    end
    enum + ")"
  end

  def self.doc_repo_types_enum
    enum = "ENUM("
    first = true
    Doc_Repo_Types.each do |k, v|
      enum += "," if !first
      enum += "'#{v}'"
      first = false
    end
    enum + ")"
  end

  def self.build_from_input (api_user_info, input)
    if (input.blank? || api_user_info.input_owner.nil?)
      raise Api::V1::ApiError.new(:bad_request, "Doc container input missing")
    end

    input.symbolize_keys!

    repo_type = (input[:repo_type] || Doc_Repo_Types[:embedded])
    if (input.key?(:text) && (repo_type != Doc_Repo_Types[:embedded]))
      raise Api::V1::ApiError.new(:bad_request, "Repo type and input text are incompatible (use embedded)")
    end
    input[:repo_type] = repo_type

    input[:owner_id] = api_user_info.input_owner.id
    input[:org_id] = api_user_info.input_org.nil? ? nil : api_user_info.input_org.id

    if (input.key?(:data_credentials_id))
      data_credentials = DataCredentials.find(input[:data_credentials_id])
      if (!Ability.new(api_user_info.input_owner).can?(:read, data_credentials))
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data credentials")
      end
    end

    if (!api_user_info.user.super_user? && input.key?(:public))
      raise Api::V1::ApiError.new(:bad_request, "Input cannot include public attribute")
    end

    tags = input[:tags]
    input.delete(:tags)

    doc_container = DocContainer.create(input)

    ResourceTagging.add_owned_tags(doc_container, { tags: tags }, api_user_info.input_owner)

    return doc_container
  end

  def update_mutable! (api_user_info, input)
    return if (input.blank? || api_user_info.nil?)

    tags = input.delete(:tags)

    self.name = input[:name] if input.key?(:name)
    self.description = input[:description] if input.key?(:description)
    self.owner = api_user_info.input_owner if (self.owner != api_user_info.input_owner)
    self.org = api_user_info.input_org if (self.org != api_user_info.input_org)

    if (input.key?(:data_credentials_id))
      data_credentials = DataCredentials.find(input[:data_credentials_id])
      if (!Ability.new(api_user_info.input_owner).can?(:read, data_credentials))
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to data credentials")
      end
      self.data_credentials = data_credentials
    end

    if (input.key?(:public))
      if (!api_user_info.user.super_user?)
        raise Api::V1::ApiError.new(:method_not_allowed, "Input cannot include public attribute")
      end
      self.public = input[:public]
    end

    self.repo_type = input[:repo_type] if !input[:repo_type].blank?
    if (!self.embedded?)
      if (!input[:text].blank?)
        raise Api::V1::ApiError.new(:bad_request, "Repo type and input text are incompatible (use embedded)")
      end
      self.text = nil
    end

    self.repo_config = input[:repo_config] if input.key?(:repo_config)
    self.text = input[:text] if (input.key?(:text) && self.embedded?)

    self.save!

    ResourceTagging.add_owned_tags(self, { tags: tags }, api_user_info.input_owner)
  end

  def embedded?
    (self.repo_type == Doc_Repo_Types[:embedded])
  end

  def text
    case self.repo_type
    when Doc_Repo_Types[:github]
      return nil if self.data_credentials.nil?
      return GithubService.get_doc(self)
    else
      txt = self.inflate(super)
    end
    return txt
  end

  def text=(txt)
    begin
      ctxt = Base64.encode64(Zlib::Deflate.deflate(txt.encode(TEXT_ENCODING)))
    rescue
      # Ignore exception and save the raw txt
      ctxt = txt
    end
    super(ctxt)
  end

  protected

  def inflate (txt)
    begin
      result = Zlib::Inflate.inflate(Base64.decode64(txt)).force_encoding(TEXT_ENCODING)
    rescue
      result = txt
    end
    return result
  end
end
