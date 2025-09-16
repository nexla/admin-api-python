class EnvironmentUrl
  include Singleton

  attr_writer :webhook_host, :nexset_api_host, :file_upload_host if Rails.env.test?

  def webhook_host
    init
    @webhook_host
  end

  def webhook_url (data_source, api_key = nil)
    init
    return nil unless @webhook_host.present? && data_source.is_a?(DataSource)
    "https://%s/data%s/%s?api_key=%s" % ([@webhook_host] + self.resource_info(data_source, api_key))
  end

  def file_upload_host
    init
    @file_upload_host
  end

  def file_upload_url (data_source, api_key = nil)
    init
    return nil unless @file_upload_host.present? && data_source.is_a?(DataSource)
    "https://%s/file%s/%s?api_key=%s" % ([@file_upload_host] + self.resource_info(data_source, api_key))
  end

  def nexset_api_host
    init
    @nexset_api_host
  end

  def nexset_api_url (resource, api_key = nil)
    init
    return "https://%s" % [@nexset_api_host] if resource.is_a?(Org)
    return nil unless @nexset_api_host.present? && resource.is_a?(DataSet)
    "https://%s/sync%s/%s?api_key=%s" % ([@nexset_api_host] + self.resource_info(resource, api_key))
  end

  def ai_web_server_host
    init
    @ai_web_server_host
  end

  def ai_web_server_url (data_source)
    init
    return nil unless @ai_web_server_host.present? && data_source.is_a?(DataSource)
    "https://%s/flow/rag/%s" % ([@ai_web_server_host, data_source.flow_node_id])
  end

  def adaptive_flows_host
    init
    @adaptive_flows_host
  end

  def adaptive_flow_url (data_source)
    init
    return nil unless @adaptive_flows_host.present? && data_source.is_a?(DataSource)
    "https://%s/flows/%s/run_profiles/activate" % ([@adaptive_flows_host, data_source.origin_node_id])
  end

  def api_web_server_host
    init
    @api_web_server_host
  end

  def api_web_server_url (data_source)
    init
    return nil unless @api_web_server_host.present? && data_source.is_a?(DataSource)
    "https://%s/api_server/flow/%s" % ([@api_web_server_host, data_source.flow_node_id])
  end

  private

  def init
    @init ||= nil
    return if @init
    @webhook_host = ENV["WEBHOOK_HOST"]
    @nexset_api_host = ENV["NEXSET_API_HOST"]
    @file_upload_host = ENV["FILE_UPLOAD_HOST"]
    @ai_web_server_host = ENV["AI_WEB_SERVER_HOST"]
    @adaptive_flows_host = ENV["ADAPTIVE_FLOWS_HOST"]
    @api_web_server_host = ENV["API_WEB_SERVER_HOST"]
    @init = true
  end

  def resource_info (res, api_key = nil)
    cluster = res.org&.cluster
    uid = "-#{cluster.dataplane_key}" if (cluster.present? && cluster.supports_multi_dataplane? && !cluster.is_default?)
    uid ||= ""
    api_key = api_key.respond_to?(:api_key) ? api_key.api_key : res.api_keys.first&.api_key
    api_key ||= ""
    [uid, res.id, api_key]
  end
end
