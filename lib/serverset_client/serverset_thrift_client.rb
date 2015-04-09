require 'logger'
require 'thrift_client'

module ServersetClient
  class ServersetThriftClient < AbstractThriftClient
    attr_reader :serverset_path
    attr_reader :logger
    attr_reader :endpoint
    attr_reader :watcher

    def initialize(client_class, zk_host, serverset_path, options = {})
      options = options.dup
      @logger = options.delete(:logger) || ServersetClient::NullLogger.new
      @endpoint = options.delete(:endpoint) || nil
      watcher_options = {
        :zk => options.delete(:zk),
        :max_servers => options.delete(:max_servers),
        :logger => logger
      }
      @serverset_path = serverset_path
      @watcher = ServersetWatcher.new(zk_host, serverset_path, watcher_options)
      observer = ServersetWatcherObserver.new(self, @endpoint) do |server_list|
        @server_list = server_list
        @current_server = server_list.first
      end
      @watcher.add_observer(observer)
      super(client_class, [], options)
    end

    def update_serverset!
      @watcher.check!
    end

    private

    def handled_proxy(method_name, *args)
      @watcher.check!
      super(method_name, *args)
    end
  end

  class ServersetWatcherObserver
    def initialize(client, endpoint, &updater)
      @client = client
      @endpoint = endpoint
      @updater = updater
    end

    def update(services)
      if services.empty?
        @client.logger.info("Ignoring empty serverset for #{@client.serverset_path}")
      else
        @client.logger.info("Loading new servers for #{@client.serverset_path}")
        thrift_servers = services.values.map { |s| s.thrift_server(@client, @endpoint) }.compact.shuffle
        @client.disconnect!
        @updater.call(thrift_servers)
      end
    end
  end
end
