require 'multi_json'
require 'thrift'

module ServersetClient
  class Service
    attr_reader :member_id # member_00001
    attr_reader :service_endpoint # 127.0.0.1:9999
    attr_reader :additional_endpoint_hash # { :host => 127.0.0.1:2132, :admin => 127.0.0.1:2352, :http => 127.0.0.1:80 }
    attr_reader :shard # 0

    def self.from_json(server,data)
      instance = MultiJson.load(data)
      endpoint = instance['serviceEndpoint']
      additional_endpoints = Hash[instance['additionalEndpoints'].collect { |endpoint_type, endpoint_json|
        [endpoint_type,"#{endpoint_json['host']}:#{endpoint_json['port']}"]
      }] if instance['additionalEndpoints']
      Service.new server, "#{endpoint['host']}:#{endpoint['port']}", additional_endpoints
    end

    def self.from_thrift(server,data)
      instance = Thrift::Deserializer.new.deserialize(Twitter::Thrift::ServiceInstance.new, data)
      endpoint = instance.serviceEndpoint
      additional_endpoints = Hash[instance.additionalEndpoints.collect { |endpoint_type, endpoint_thrift|
        [endpoint_type,"#{endpoint_thrift.host}:#{endpoint_thrift.port}"]
      }] if instance.additionalEndpoints
      Service.new server, "#{endpoint.host}:#{endpoint.port}", additional_endpoints
    end

    def initialize(member_id, service_endpoint, additional_endpoint_hash = {}, shard = nil)
      @member_id = member_id
      @service_endpoint = service_endpoint
      @additional_endpoint_hash = additional_endpoint_hash || {}
      @shard = shard && shard.to_i
    end

    def to_json
      h = {
        :serviceEndpoint => endpoint_as_hash(service_endpoint),
        :additionalEndpoints => additional_endpoints_as_hash,
        :status => 'ALIVE'
      }
      h.merge!(:shard => shard) if shard
      MultiJson.dump(h)
    end

    def thrift_server(client = nil, endpoint_type = nil)
      endpoint = endpoint_type ? @additional_endpoint_hash[endpoint_type] : @service_endpoint
      return nil unless endpoint
      if ServersetThriftClient::Server.instance_method(:initialize).arity == 1
        # thrift_client < 0.8.3
        ServersetThriftClient::Server.new(endpoint)
      else
        ServersetThriftClient::Server.new(endpoint, client.client_class, client.options)
      end
    end

  protected
    def endpoint_as_hash(endpoint)
      (host, port) = endpoint.split(':')
      { :host => host, :port => port.to_i }
    end

    def additional_endpoints_as_hash
      additional_endpoint_hash.reduce({}) do |h, (name, endpoint)|
          h.merge(name => endpoint_as_hash(endpoint))
      end
    end
  end
end
