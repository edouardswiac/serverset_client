require 'logger'
require 'monitor'
require 'observer'
require 'multi_json'
require 'set'
require 'zk'

module ServersetClient

  class ServersetWatcher
    include Observable

    attr_reader :zk_host
    attr_reader :serverset_path
    attr_reader :logger
    attr_reader :max_servers

    def initialize(zk_host, serverset_path, options = {})
      @logger = options[:logger] || Logger.new(STDOUT)
      @zk_options = options[:zk] || {}
      @max_servers = options[:max_servers]
      @serverset_path = serverset_path
      @zk_host = zk_host

      @init_lock = Monitor.new
      # @local_serverset is keyed by String child names
      # and mapped to ServersetClient::Service instances
      @local_serverset = {}
      @local_serverset_dirty = false
      @local_serverset_lock = Monitor.new
    end

    def check!
      init_zk
      @local_serverset_lock.synchronize do
        if @local_serverset_dirty
          @local_serverset_dirty = false
          changed
        end
        notify_observers(@local_serverset)
      end
    end

    private

    def init_zk
      @init_lock.synchronize do
        begin
          return if @zk
          @zk = ZK.new(zk_host, @zk_options)
          register_zk_handlers
          logger.info("ZK client initialized for #{serverset_path}")
          reload_children
        rescue
          @zk.close! if @zk #close! clears all event subscription
          @zk = nil
          raise
        end
      end
    end

    def register_zk_handlers
      @serverset_path_sub = @zk.register(serverset_path, &method(:reload_children))
      @on_connected_sub = @zk.on_connected(&method(:reload_children))
    end

    def reload_children(event = nil)
      logger.debug("ZK event for #{serverset_path}: #{event.inspect}") if event
      children = @zk.children(@serverset_path, :watch => true)
      new_children = select_new_children(children)
      new_services = {}
      new_children.each do |ea|
        child_service = get_child_service(ea)
        new_services[ea] = child_service if child_service
      end

      unless new_services.empty?
        @local_serverset_lock.ssynchronize do
          @local_serverset.merge!(new_services)
          @local_serverset_dirty = true
        end
      end
    end

    def select_new_children(zk_children)
      @local_serverset_lock.synchronize do
        children = zk_children.select { |ea| ea =~ /\Amember_/ }
        dropped_children = @local_serverset.keys - children
        unless dropped_children.empty?
          logger.debug("These services are now offline: #{@serverset_path}/#{dropped_children.inspect}")
          dropped_children.each { |ea| @local_serverset.delete(ea) }
          @local_serverset_dirty = true
        end

        new_children = children - @local_serverset.keys
        if max_servers
          new_children_needed = max_servers - @local_serverset.size
          new_children = new_children.sample(new_children_needed)
        end
        logger.debug("These services are new: #{@serverset_path}/#{new_children.inspect}")
        new_children
      end
    end

    def get_child_service(child)
      data, stat = @zk.get("#{@serverset_path}/#{child}")
      if data[0..0] == '{'
        logger.debug("Got JSON for #{serverset_path}/#{child}: #{data}")
        Service.from_json child, data
      else
        logger.debug("Got Thrift for #{serverset_path}/#{child}: #{instance.inspect}")
        Service.from_thrift child, data
      end
    rescue ZK::Exceptions::NoNode
      nil
    end
  end
end
