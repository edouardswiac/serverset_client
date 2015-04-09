require './test/test_helper'

describe ServersetThriftClient do
  after do
    stop_servers
  end

  describe "Normal usage" do
    before do
      start_servers([
        { :node => "/test", :parent => true },
        { :node => "/test/member_0000", :host => "127.0.0.1", :port => 11461 },
        { :node => "/test/member_0001", :host => "127.0.0.1", :port => 11462 },
        { :node => "/test/member_0002", :host => "127.0.0.1", :port => 11463 }
      ])
    end

    it "initializes Zookeeper correctly with the correct servers" do
      client = ServersetThriftClient.new(Greeter::Client, "127.0.0.1:21811", "/test", :logger => logger)
      assert_equal("hello there Tanin!", client.greeting("Tanin"))
      assert_equal(["127.0.0.1:11461", "127.0.0.1:11462", "127.0.0.1:11463"].sort, get_all_servers(client).sort)
    end

    it 'initializes Zookeeper correctly with the correct servers with update!' do
      client = ServersetThriftClient.new(Greeter::Client, '127.0.0.1:21811', '/test', :logger => logger, :prefetch => true)
      client.update_serverset!
      assert_equal(%w(127.0.0.1:11461 127.0.0.1:11462 127.0.0.1:11463).sort, get_all_servers(client).sort)
    end

    it "updates the servers correctly" do
      client = ServersetThriftClient.new(Greeter::Client, "localhost:21811", "/test", :logger => logger)
      assert_equal("hello there Tanin!", client.greeting("Tanin"))

      ZK.open(zk_server_address) do |zk|
        delete_end_point(zk, "/test/member_0000")
        delete_end_point(zk, "/test/member_0001")
        delete_end_point(zk, "/test/member_0002")

        create_end_point(zk, "/test/member_0003", "127.0.0.1", 1464)
        create_end_point(zk, "/test/member_0004", "127.0.0.1", 1465)
      end

      assert_equal("hello there Tanin!", client.greeting("Tanin"))  # make sure it works
      assert_equal(["127.0.0.1:1464", "127.0.0.1:1465"].sort, get_all_servers(client).sort)
    end
  end

  describe "Additional endpoints" do
    it "uses servers with an additional endpoint" do
      start_servers([
        { :node => "/test", :parent => true },
        { :node => "/test/member_0000", :host => "127.0.0.1", :port => 11461 },
        { :node => "/test/member_0001", :host => "127.0.0.1", :port => 11462,
          :additional => { "thrift" => { :host => "127.0.0.1", :port => 11463 } } },
      ])
      client = ServersetThriftClient.new(Greeter::Client, "localhost:21811", "/test", :logger => logger, :endpoint => "thrift")
      client.update_serverset!
      assert_equal(["127.0.0.1:11463"].sort, get_all_servers(client).sort)
    end
  end

  describe "Resilience" do
    it "doesn't raise an error when initialize, but raises an error when invoking, then it works if everything is ok" do
      start_servers([])
      client = ServersetThriftClient.new(Greeter::Client, "localhost:21811", "/test", :logger => logger)

      exception = assert_raises ZK::Exceptions::NoNode do
        client.greeting("Tanin")
      end

      ZK.open(zk_server_address) do |zk|
        zk.create("/test")
        create_end_point(zk, "/test/member_0000", "127.0.0.1", 11464)
      end

      assert_equal("hello there Tanin!", client.greeting("Tanin"))
      assert_equal(["127.0.0.1:11464"].sort, get_all_servers(client).sort)
    end

    it 'should ignore non-member nodes' do
      start_servers([])
      client = ServersetThriftClient.new(Greeter::Client, "localhost:21811", "/test", :logger => logger)

      ZK.open(zk_server_address) do |zk|
        zk.create("/test")
        create_end_point(zk, "/test/member_0000", "127.0.0.1", 11464)
        zk.create("/test/vector_0001", :data =>  MultiJson.dump({
          :vector => [{
            :weight => 6.5,
            :select => "localhost:11464"
          }]
        }))
      end

      assert_equal("hello there Tanin!", client.greeting("Tanin"))
      assert_equal(["127.0.0.1:11464"].sort, get_all_servers(client).sort)
    end

    it 'should handle mass removal and additions' do
      num_servers = 100
      num_removed = 10

      start_servers([])
      client = ServersetThriftClient.new(Greeter::Client, 'localhost:21811', '/test', :logger => logger, :retries => 1)

      assert_raises ZK::Exceptions::NoNode do
        client.greeting("Tanin")
      end

      server_range = [*1..num_servers]
      removal_range = [*1..num_removed]

      # Additions
      ZK.open(zk_server_address) do |zk|
        zk.create("/test")
        server_range.each { |i|
          create_fake_end_point(zk, "/test/member_#{i}", '127.0.0.1', i)
        }
      end

      force_serverset_refresh client
      servers = get_all_servers(client).sort
      server_range.each { |i|
        assert(servers.include?("127.0.0.1:#{i}"), "servers should include 127.0.0.1:#{i}")
      }

      # Mass removal
      removed_servers = []
      removal_range.each { |i|
        ZK.open(zk_server_address) do |zk|
          random_server = rand(num_servers) + 1
          begin
            delete_end_point(zk, "/test/member_#{random_server}")
            removed_servers << random_server
          rescue ZK::Exceptions::NoNode
            # sample isn't so samply
          end
        end
      }
      sleep 0.05

      force_serverset_refresh client
      servers = get_all_servers(client).sort
      removed_servers.each { |i|
        refute(servers.include?("127.0.0.1:#{i}"), "servers should not include 127.0.0.1:#{i}")
      }
    end
  end

  describe 'No servers' do
    before do
      start_servers([{ :node => '/test', :parent => true }])
    end

    it 'initialises with no servers' do
      client = ServersetThriftClient.new(Greeter::Client, 'localhost:21811', '/test', :logger => logger)
      assert_equal(%w().sort, get_all_servers(client).sort)
      assert_raises(ThriftClient::NoServersAvailable) do
        client.greeting("Tanin")
      end
      assert_equal(%w().sort, get_all_servers(client).sort)
    end

    it 'should ignore empty server set if all servers removed' do
      ZK.open(zk_server_address) do |zk|
        create_end_point(zk, '/test/member_0000', '127.0.0.1', 11464)
        create_end_point(zk, '/test/member_0001', '127.0.0.1', 11465)
      end
      client = ServersetThriftClient.new(Greeter::Client, 'localhost:21811', '/test', :logger => logger)
      assert_equal("hello there Tanin!", client.greeting("Tanin"))
      assert_equal(%w(127.0.0.1:11464 127.0.0.1:11465).sort, get_all_servers(client).sort)
      ZK.open(zk_server_address) do |zk|
        delete_end_point(zk, '/test/member_0000')
        delete_end_point(zk, '/test/member_0001')
        sleep 0.1
      end
      assert_equal(%w(127.0.0.1:11464 127.0.0.1:11465).sort, get_all_servers(client).sort)
    end
  end
end

# Forcing a refresh of the server set. The backend servers haven't actually been started, just
# registered in zk, so there's an error here to be handled here.
def force_serverset_refresh(client)
  client.greeting("Tanin")
rescue Greeter::Client::TransportException, ThriftClient::NoServersAvailable
  nil
end
