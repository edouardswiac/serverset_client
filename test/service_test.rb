require './test/test_helper'
require 'json'

describe Service do
  before do
    @member = 'test_member_0000'
    @simple_server = { :host => '127.0.0.1', :port => 1234 }
    @simple_endpoint = { :serviceEndpoint => @simple_server }
    @complex_endpoint = { :serviceEndpoint => @simple_server, :additionalEndpoints => { 'http' => @simple_server, 'admin' => @simple_server}}
    @client = ServersetThriftClient.new(Greeter::Client, "127.0.0.1:21811", "/test")
  end
  describe 'service building' do
    describe 'JSON' do
      it 'should build from json' do
        service = Service.from_json @member, @simple_endpoint.to_json
        assert_equal(@member, service.member_id)
        assert_equal('127.0.0.1:1234', service.service_endpoint)
        assert_equal(0, service.additional_endpoint_hash.size)
      end

      it 'should build with additional endpoints from json' do
        service = Service.from_json @member, @complex_endpoint.to_json
        assert_equal(@member, service.member_id)
        assert_equal('127.0.0.1:1234', service.service_endpoint)
        assert_equal(2, service.additional_endpoint_hash.size)
        assert_equal('127.0.0.1:1234', service.additional_endpoint_hash['http'])
        assert_equal('127.0.0.1:1234', service.additional_endpoint_hash['admin'])
      end
    end

    describe 'Thrift' do
      it 'should build from thrift' do
        service = Service.from_thrift @member, to_thrift(@simple_endpoint)
        assert_equal(@member, service.member_id)
        assert_equal('127.0.0.1:1234', service.service_endpoint)
        assert_equal(0, service.additional_endpoint_hash.size)
      end

      it 'should build complex from thrift' do
        service = Service.from_thrift @member, to_thrift(@complex_endpoint)
        assert_equal(@member, service.member_id)
        assert_equal('127.0.0.1:1234', service.service_endpoint)
        assert_equal(2, service.additional_endpoint_hash.size)
        assert_equal('127.0.0.1:1234', service.additional_endpoint_hash['http'])
        assert_equal('127.0.0.1:1234', service.additional_endpoint_hash['admin'])
      end
    end

    describe 'Client usage' do
      it 'should return usable thrift client server' do
        thrift_server = Service.from_json(@member, @complex_endpoint.to_json).thrift_server(@client)
        assert_equal("127.0.0.1:1234", thrift_server.to_s)
      end
    end
  end
end

def to_thrift(server_hash)
  Thrift::Serializer.new.serialize(to_server_instance(server_hash))
end

def to_server_instance(server_hash)
  service = Twitter::Thrift::ServiceInstance.new
  service.serviceEndpoint = to_endpoint(server_hash[:serviceEndpoint])
  service.additionalEndpoints = Hash[server_hash[:additionalEndpoints].map { |type,endpoint|
    [type,to_endpoint(endpoint)]
  }] if server_hash[:additionalEndpoints]
  service
end

def to_endpoint(endpoint_hash)
  endpoint = Twitter::Thrift::Endpoint.new
  endpoint.host = endpoint_hash[:host]
  endpoint.port = endpoint_hash[:port]
  endpoint
end