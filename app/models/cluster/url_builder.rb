class Cluster
  class UrlBuilder
    attr_accessor :cluster_endpoint

    def initialize (cluster_endpoint)
      self.cluster_endpoint = cluster_endpoint
    end

    def base_url
      if hostname.blank?
        raise Api::V1::ApiError.new(:internal_server_error,
          message: "Can't build URL for cluster endpoint (Org ID #{cluster_endpoint.org_id}, service #{cluster_endpoint.service})")
      end

      opts = {
        schema: cluster_endpoint.protocol,
        host: hostname,
        path: path
      }
      port = cluster_endpoint.port
      port = port.present? ? port.to_i : 80
      opts[:port] = port if (port != 80)
      begin
        builder_class.build(opts).to_s
      rescue URI::InvalidURIError => e
        raise Api::V1::ApiError.new(:internal_server_error,
          message: "Can't build URL for cluster endpoint (Org ID #{cluster_endpoint.org_id}, service #{cluster_endpoint.service})")
      end
    end

    private

    def builder_class
      URI.scheme_list.fetch(cluster_endpoint.protocol.upcase, URI::Generic)
    end

    def hostname
      # Remove after DB data is migrated
      hostname, _ = cluster_endpoint.host.split('/', 2)

      hostname
    end

    def path
      # Remove after DB data is migrated
      _, path_from_hostname = cluster_endpoint.host.split('/', 2)

      [path_from_hostname, cluster_endpoint.context].compact.join('/').tap do |path|
        if path.present? && !path.start_with?('/')
          path.prepend('/') # Always pass absolut path
        end
      end
    end
  end
end
