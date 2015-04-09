require './test/test_helper'

describe Serverset do
  after do
    stop_servers
  end

  describe "Normal usage" do
    before do
      start_servers([
        { :node => "/test", :parent => true },
        { :node => "/test/member_0000", :host => "127.0.0.1", :port => 11461 },
        { :node => "/test/member_0001", :host => "127.0.0.1", :port => 11462,
          :additional => { "http" => { :host => "127.0.0.1", :port => 11464 } } },
        { :node => "/test/member_0002", :host => "127.0.0.1", :port => 11463,
          :additional => { "http" => { :host => "127.0.0.1", :port => 11465 } } },
      ])
    end

    it "initializes Zookeeper correctly with the correct servers" do
      serverset = Serverset.new("127.0.0.1:21811", "/test", :logger => logger)
      assert_equal(["127.0.0.1:11461", "127.0.0.1:11462", "127.0.0.1:11463"].sort, serverset.hosts.sort)
      assert_equal(["127.0.0.1:11464", "127.0.0.1:11465"].sort, serverset.hosts('http').sort)
    end

    it "updates the servers correctly" do
      serverset = Serverset.new("127.0.0.1:21811", "/test", :logger => logger)

      ZK.open(zk_server_address) do |zk|
        delete_end_point(zk, "/test/member_0000")
        delete_end_point(zk, "/test/member_0001")
        delete_end_point(zk, "/test/member_0002")

        create_end_point(zk, "/test/member_0003", "127.0.0.1", 1464)
        create_end_point(zk, "/test/member_0004", "127.0.0.1", 1465,
                         { "http" => { :host => "127.0.0.1", :port => 1466 } })
      end

      assert_equal(["127.0.0.1:1464", "127.0.0.1:1465"].sort, serverset.hosts.sort)
      assert_equal(["127.0.0.1:1466"].sort, serverset.hosts('http').sort)
    end
  end

  describe 'No servers' do
    before do
      start_servers([{ :node => "/test", :parent => true }])
    end

    it 'initialises with no servers' do
      serverset = Serverset.new('127.0.0.1:21811', '/test', :logger => logger)
      assert_equal(%w().sort, serverset.hosts.sort)
    end

    it 'handles the removal of all servers' do
      ZK.open(zk_server_address) do |zk|
        create_end_point(zk, '/test/member_0000', '127.0.0.1', 11464)
        create_end_point(zk, '/test/member_0001', '127.0.0.1', 11465)
      end
      serverset = Serverset.new('127.0.0.1:21811', '/test', :logger => logger)
      assert_equal(%w(127.0.0.1:11464 127.0.0.1:11465).sort, serverset.hosts.sort)
      ZK.open(zk_server_address) do |zk|
        delete_end_point(zk, '/test/member_0000')
        delete_end_point(zk, '/test/member_0001')
        sleep 0.05
      end
      assert_equal(%w().sort, serverset.hosts.sort)
    end
  end

  describe 'join serverset' do
    before do
      start_servers([{ :node => "/test", :parent => true }])
    end

    it 'should register a new host on serverset' do
      serverset = Serverset.new('127.0.0.1:21811', '/test', :logger => logger)
      serverset.join('127.0.0.1:3000', {'aurora' => '127.0.0.1:3000', 'health' => '127.0.0.1:3001'}, 0)
      assert_equal(%w(127.0.0.1:3000).sort, serverset.hosts.sort)
      new_service = serverset.services.first[1]
      assert_equal('127.0.0.1:3000', new_service.service_endpoint)
      assert_match(/\Amember_\d+/, new_service.member_id)
      assert_equal('127.0.0.1:3001', new_service.additional_endpoint_hash['health'])
    end
  end

  describe 'leave serverset' do
    before do
      start_servers([{ :node => "/test", :parent => true },
                     { :node => "/test/member_0000", :host => "127.0.0.1", :port => 11461}])
    end

    it 'should remove a host from serverset' do
      serverset = Serverset.new('127.0.0.1:21811', '/test', :logger => logger)
      assert_equal(%w(127.0.0.1:11461).sort, serverset.hosts.sort)
      service = serverset.services.first[1]
      serverset.leave(service.member_id)
      3.times { break if serverset.hosts.empty? ; sleep 0.1 }
      assert_equal(true, serverset.hosts.empty?)
    end
  end
end
