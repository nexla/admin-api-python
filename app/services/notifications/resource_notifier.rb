module Notifications
  class ResourceNotifier

    def initialize(resource, event, payload = {})
      @resource = resource
      @event = event
      @payload = payload
    end

    # use to avoid nested notifications
    def self.exclusive_for(resource)
      return if resource.nil?

      set_exclusive = _exclusive_for.nil? && resource.present?

      self._exclusive_for = resource if set_exclusive

      begin
        yield
      ensure
        self._exclusive_for = nil if set_exclusive
      end
    end

    def self.reset_exclusive_resource
      self._exclusive_for = nil
    end

    def call
      return if notifications_disabled
      return if self.class._exclusive_for.present? && self.class._exclusive_for != resource

      # Let's assume DataFlow always starts with a DataSource!
      if self.resource.class.name == 'DataFlow'
        resource = self.resource.resource
        resource_type = 'SOURCE'
      else
        resource = self.resource
        resource_type =  resource_type_name(resource)
      end

      context_payload = {
        name: resource.name,
        owner_id: resource.owner_id,
        owner_full_name: resource.owner.full_name,
        owner_email: resource.owner.email,
        org_id: resource.org_id
      }

      context_payload.merge!(self.payload) if self.payload.present?

      payload = {
        context: context_payload,
        event_type: event.to_s.upcase,
        resource_type: resource_type,
        resource_id: resource.id,
        event_source: resource.name
      }

      ResourceEventNotificationWorker.perform_async(resource.org_id, payload.as_json)
    end

    private
    cattr_accessor :_exclusive_for, :notifications_disabled

    attr_reader :resource, :event, :payload

    def resource_type_name(resource)
      case resource.class.name
      when 'DataSet' then 'DATASET'
      when 'DataSource' then 'SOURCE'
      when 'DataSink' then 'SINK'
      else
        resource.class.name.underscore.upcase
      end
    end

  end
end