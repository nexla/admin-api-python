class Team < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AccessControls::Membership
  include AccessControls::Accessor
  include AuditLog
  include Docs
  include MembershipAssociationsTrail
  include Accessible

  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org

  has_many :team_memberships
  has_many :members, class_name: "User", source: :user, through: :team_memberships, dependent: :destroy

  acts_as_taggable_on :tags
  def tags_list
    self.tags.map(&:name)
  end
  alias_method :tag_list, :tags_list

  before_save do
    m = self.members.map(&:id)
    self.members = []
    self.members = User.find(m.uniq)
  end

  def self.build_from_input(input, user, org = nil)
    members = input[:members]
    input.delete(:members)

    input[:owner] = user
    input[:org] = org
    team = nil
    self.transaction do
      team = Team.create(input)
      team.update_members(members, :reset)
    end
    return team
  end

  def org_member? (org)
    self.org == org
  end
  
  def orgs
    self.org
  end
  
  def update_mutable! (request, user, input)
    self.name = input[:name] if !input[:name].blank?
    self.description = input[:description] if input.key?(:description)
    self.update_members(input[:members], :add) if input.key?(:members)
    self.save!
  end

  def update_members (members, mode)
    return if members.blank? && (mode != :reset)

    members = [] if members.nil?
    members = [members] if members.class != Array
    members_2 = []
    admins = []
    not_admins = []

    members.each do |m|
      m.symbolize_keys!
      user = User.find_by_id(m[:id]) if m.key?(:id)
      user ||= User.find_by_email(m[:email]) if m.key?(:email)
      if (user.nil?)
        raise Api::V1::ApiError.new(:bad_request, "User invalid or not found: #{m.inspect}")
      end
      if (!self.org.nil? && !user.org_member?(self.org))
        raise Api::V1::ApiError.new(:bad_request, "User is not a member of team's organization: #{m.inspect}")
      end
      members_2 << user
      if (m[:admin])
        admins << user
      else
        not_admins << user
      end
    end

    members_2 = members_2.uniq

    if (mode == :remove)
      self.members.delete(members_2)
      self.log_members_event(:membership_removed, self, members, members_2)
      return
    end

    if (mode == :add)
      added_members = members_2 - self.members.to_a
      self.members = (self.members.to_a + members_2).uniq
      filtered = members.filter do |member|
        added_user = added_members.find { |m| m.id == member[:id] || m.email == member[:email] }
        added_user.present?
      end
      self.log_members_event(:membership_added, self, filtered, members_2, save_is_admin: true)
    else
      existing_members = self.members.to_a
      self.members = members_2

      added = members_2 - existing_members
      removed = existing_members - self.members.to_a
      self.log_members_event(:membership_removed, self, removed.map{|m| { id: m.id } }, removed)
      added_log_params = added.map do |user|
        { id: user.id, admin: admins.include?(user)}
      end
      self.log_members_event(:membership_added, self, added_log_params, members_2, save_is_admin: true)
    end

    admins.each do |u|
      self.add_admin(u)
    end

    not_admins.each do |u|
      self.remove_admin(u)
    end
  end

  def destroy
    self.transaction do
      self.delete_acl_entries
      super
    end
  end

  def projects (access_role = :all, org_ignored = nil)
    Project.accessible(self, access_role, self.org)
  end

end
