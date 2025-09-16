class Notification < ApplicationRecord
  self.primary_key = :id

  include Api::V1::Schema
  include AccessControls::Standard

  validates :level, presence: true
  belongs_to :owner, class_name: "User", foreign_key: "owner_id", required: true
  belongs_to :org

  Resource_Types = {
    :source             => 'SOURCE',
    :pub                => 'PUB',
    :sub                => 'SUB',
    :dataset            => 'DATASET',
    :sink               => 'SINK',
    :user               => 'USER',
    :org                => 'ORG',
    :custom_data_flow   => 'CUSTOM_DATA_FLOW',
    :data_flow          => 'DATA_FLOW',
    :catalog_config     => 'CATALOG_CONFIG',
    :approval_step      => 'APPROVAL_STEP'
  }

  Levels = API_NOTIFICATION_LEVELS

  MESSAGE_SIZE_LIMIT = (64.kilobytes - 1)

  def self.resource_types_enum
    enum = "ENUM("
    first = true
    Resource_Types.each do |k, v|
      enum += "," if !first
      enum += "'#{v}'"
      first = false
    end
    enum + ")"
  end

  def self.validate_resource_type (type_str)
    return nil if !type_str.is_a?(String)
    return nil if Resource_Types.find { |sym, str| str == type_str }.nil?
    type_str
  end

  def self.build_from_input (user, org, input)
    res_type = nil
    if (input.key?(:resource_type))
      res_type = input[:resource_type]
      res_type = res_type.upcase if res_type.is_a?(String)
      res_type = Notification.validate_resource_type(res_type)
      raise Api::V1::ApiError.new(:bad_request, "Invalid resource type") if res_type.nil?
      raise Api::V1::ApiError.new(:bad_request, "Missing resource id") if !input.key?(:resource_id)
    end

    res = nil
    if (input.key?(:resource_id))
      raise Api::V1::ApiError.new(:bad_request, "Missing resource type") if res_type.nil?
      res_id = input[:resource_id]
      case res_type
        when Resource_Types[:source]
          res = DataSource.find_by(id: res_id)
        when Resource_Types[:dataset]
          res = DataSet.find_by(id: res_id)
        when Resource_Types[:sink]
          res = DataSink.find_by(id: res_id)
        when Resource_Types[:user]
          res = User.find_by(id: res_id)
        when Resource_Types[:org]
          res = Org.find_by(id: res_id)
      end
    end

    input[:resource_type] = res_type
    input[:resource_id] = res.nil? ? nil : res.id

    if input[:message] && input[:message].length >= MESSAGE_SIZE_LIMIT
      input[:message] = input[:message][0..(MESSAGE_SIZE_LIMIT - 1)]
    end

    if (user.super_user?)
      if (res.nil?)
        raise Api::V1::ApiError.new(:bad_request, "Missing owner_id") if input[:owner_id].blank?
        u = User.find(input[:owner_id])
        o = nil
        if (!input[:org_id].blank?)
          o = Org.find(input[:org_id])
          raise Api::V1::ApiError.new(:bad_request, "User not in org") if !u.org_member?(o)
        end
        input[:owner_id] = u.id
        input[:org_id] = o.nil? ? nil : o.id

      elsif(!input.key?(:owner_id) || !input.key?(:org_id))
        if res.is_a?(Org)
          input[:owner_id] = res.owner_id
          input[:org_id] = res.id
        elsif res.is_a?(User)
          input[:owner_id] = res.id
          input[:org_id] = res.default_org.id
        else
          input[:owner_id] = res.owner.id
          input[:org_id] = res.org.id
        end
      end
    else
      input[:owner_id] = user.id
      input[:org_id] = org.nil? ? nil : org.id
      if (!res.nil? && !Ability.new(user).can?(:read, res))
        raise Api::V1::ApiError.new(:forbidden, "Unauthorized access to resource")
      end
    end

    ts = input.delete(:ts)
    if ts && ts > 10_000_000_000 # If ts is in milliseconds, convert to seconds
      ts = ts.to_f / 1000
    end
    input[:timestamp] =  (ts && Time.at(ts)) || Time.now

    notification = Notification.create(input)
    notification.save!
    return notification
  end

  ARCHIVE_BEFORE_DAYS = (Rails.env.production? || Rails.env.staging?) ? 365 : 30

  def self.archive_table_name
    NotificationsArchive.table_name
  end

  def self.has_archive?
    NotificationsArchive.has_archive?
  end

  def self.archive (time = nil)
    # Archives all notifications older than ARCHIVE_BEFORE_DAYS.
    # This is intended to be run periodically via a chron task.
    # The time argument is for testing, and does not need to be 
    # supplied during normal use
    return if !self.has_archive?

    before_count = NotificationsArchive.count
    archive_before = (time || ARCHIVE_BEFORE_DAYS.days.ago)

    Notification.where("created_at < ?", archive_before).find_in_batches do |group|
      copy_sql = %{
        INSERT INTO `#{self.archive_table_name}` (
          SELECT * FROM `#{self.table_name}` WHERE id <= #{group.last.id}
        );
      }
      delete_sql = "DELETE FROM `#{self.table_name}` WHERE id <= #{group.last.id};"
      ActiveRecord::Base.connection.execute(copy_sql)
      ActiveRecord::Base.connection.execute(delete_sql)
    end

    return (NotificationsArchive.count - before_count)
  end

  def mark_read
    self.read_at = Time.now
    self.save!
  end

  def mark_unread
    self.read_at = nil
    self.save!
  end

  def resource
    return nil if resource_type.nil? || resource_id.nil?

    case resource_type
    when Resource_Types[:source]
      DataSource.find_by(id: resource_id)
    when Resource_Types[:dataset]
      DataSet.find_by(id: resource_id)
    when Resource_Types[:sink]
      DataSink.find_by(id: resource_id)
    when Resource_Types[:user]
      User.find_by(id: resource_id)
    when Resource_Types[:org]
      Org.find_by(id: resource_id)
    end
  end

end
