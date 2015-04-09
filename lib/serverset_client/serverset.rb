require 'logger'

module ServersetClient
  class Serverset
    attr_reader :serverset_path
    attr_reader :logger
    attr_reader :watcher
    attr_reader :announcer
    attr_reader :services

    def initialize(zk_host, serverset_path, options = {})
      options = options.dup
      @logger = options.delete(:logger) || ServersetClient::NullLogger.new
      zk_options = options.delete(:zk)
      watcher_options = {
        :zk => zk_options,
        :max_servers => options.delete(:max_servers),
        :logger => logger
      }
      @serverset_path = serverset_path
      @watcher = ServersetWatcher.new(zk_host, serverset_path, watcher_options)
      @watcher.add_observer(self)

      announcer_options = {:zk => zk_options, :creds => options[:creds]}
      @announcer = Announcer.new(zk_host, serverset_path, announcer_options)

      @services = {}
    end

    def join(service_endpoint, additional_endpoints, shard, options = {})
      service = Service.new(nil, service_endpoint, additional_endpoints, shard)
      announcer.join(service, options)
    end

    def leave(member_id)
      announcer.leave(member_id)
    end

    def hosts(endpoint_type = nil)
      @watcher.check!
      @services.values.map { |service|
        endpoint_type ? service.additional_endpoint_hash[endpoint_type] : service.service_endpoint
      }.compact
    end

    def update(services)
      @services = services
    end
  end
end
