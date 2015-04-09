require 'minitest/spec'
require 'minitest/autorun'
require 'rr'

require 'thrift_client'
require 'serverset_client'
include ServersetClient

require './test/greeter/greeter'
require './test/greeter/server'

require 'fileutils'
require 'multi_json'
require 'zk-server'
require 'zk'

class MiniTest::Unit::TestCase
  include RR::Adapters::TestUnit

  ZK_PORT = 21811
  ZK_DATA_PATH = File.expand_path("../zk_server_data", __FILE__)

  def setup
    @pids = {}
  end

  def teardown
    RR.verify
    RR.reset
  end

  def logger
    @logger ||= Logger.new('test.log')
  end

  def zk_server_address
    "127.0.0.1:#{ZK_PORT}"
  end

  def start_servers(nodes)
    FileUtils.rm_rf(ZK_DATA_PATH)

    @zk_server = ZK::Server.new do |config|
      config.client_port = ZK_PORT
      config.base_dir = ZK_DATA_PATH
      config.enable_jmx = true
      config.force_sync = false
    end

    @zk_server.run

    zk = ZK.new(zk_server_address)

    nodes.each do |hash|
      if hash[:parent] == true
        zk.create(hash[:node])
      else
        create_end_point(zk, hash[:node], hash[:host], hash[:port], hash[:additional])
      end
    end

    zk.close!
  end

  def create_end_point(zk, node, host, port, additional = nil)
    zk.create(node, :data => MultiJson.dump({
      :serviceEndpoint => {:host => host, :port => port},
      :additionalEndpoints => additional || {}
    }))

    @pids[node] = Process.fork do
      Signal.trap("INT") { exit }
      Greeter::Server.new(port).serve
    end

    # Need to give the child process a moment to open the listening socket or
    # we get occasional "could not connect" errors in tests.
    sleep 0.05
  end

  def create_fake_end_point(zk, node, host, port, additional = nil)
    zk.create(node, :data => MultiJson.dump({
        :serviceEndpoint => {:host => host, :port => port},
        :additionalEndpoints => additional || {}
    }))
  end

  def delete_end_point(zk, node)
    zk.delete(node)

    # The Thrift server cannot be killed here. It gets an error.
    # It would be more robust to kill the Thrift server here though.
    # pid = @pids[node]
    # if pid
    #   Process.kill("INT", pid)
    #   Process.wait(pid)
    #   @pids.delete(node)
    # end
  end

  def stop_servers
    @zk_server.shutdown if @zk_server

    @pids.each_pair do |node, pid|
      Process.kill("INT", pid)
      Process.wait(pid)
    end

    @pids = {}
  end

  def get_all_servers(client)
    client.instance_variable_get(:@server_list).map { |c| c.instance_variable_get(:@connection_string) }
  end
end
