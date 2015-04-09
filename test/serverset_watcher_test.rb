require './test/test_helper'

describe ServersetWatcher do
  after do
    stop_servers
  end

  describe "Normal usage" do
    before do
      # We only want to expose this in tests:
      ServersetWatcher.send(:attr_reader, :local_serverset)

      start_servers([
        {:node => "/test", :parent => true},
        {:node => "/test/member_0000", :host => "127.0.0.1", :port => 11461},
        {:node => "/test/member_0001", :host => "127.0.0.1", :port => 11462},
        {:node => "/test/member_0002", :host => "127.0.0.1", :port => 11463}
      ])

      @expected_child_nodes = %w[member_0000 member_0001 member_0002]
    end

    def watcher(options = {})
      @watcher ||= ServersetWatcher.new(
        zk_server_address,
        "/test",
        options.merge(
          :zk => {:timeout => 15},
          :logger => logger
        )
      )
    end

    it "initializes Zookeeper correctly with zk options" do
      mock.proxy(ZK).new(zk_server_address, {:timeout => 15})
      watcher.check!
      child_nodes = watcher.local_serverset.keys
      child_nodes.sort.must_equal @expected_child_nodes
    end

    it 'limits child queries' do
      watcher(:max_servers => 2).check!
      child_nodes = watcher.local_serverset.keys
      child_nodes.size.must_equal 2
      (child_nodes - @expected_child_nodes).must_be_empty # all child_nodes are expected
    end

    it 'maintains the same children' do
      watcher(:max_servers => 2).check!
      first_child_nodes = watcher.local_serverset.keys
      10.times do
        watcher.check!
        watcher.local_serverset.keys.must_equal first_child_nodes
      end
    end

    it 'backfills when servers go down' do
      watcher(:max_servers => 2).check!
      first_picked_servers = watcher.local_serverset.keys
      crashed_server = first_picked_servers.first
      zk = ZK.new(zk_server_address)
      zk.delete("/test/#{crashed_server}")
      zk.close!
      watcher.send(:reload_children) # this is private, and should be.
      servers_after_crash = watcher.local_serverset.keys
      servers_after_crash.size.must_equal 2
      expected = @expected_child_nodes - [crashed_server]
      servers_after_crash.sort.must_equal expected
    end
  end
end

