require './test/test_helper'

describe NullLogger do
  before do
    start_servers([
      { :node => '/test', :parent => true },
      { :node => '/test/member_0000', :host => '127.0.0.1', :port => 11461 }
    ])
  end

  after do
    stop_servers
  end

  it 'works with null logger' do
    client = ServersetThriftClient.new(Greeter::Client, 'localhost:21811', '/test', :logger => ServersetClient::NullLogger.new)
    assert_equal('hello there Tanin!', client.greeting('Tanin'))

    ZK.open(zk_server_address) do |zk|
      delete_end_point(zk, '/test/member_0000')
      create_end_point(zk, '/test/member_0001', '127.0.0.1', 1464)
      create_end_point(zk, '/test/member_0002', '127.0.0.1', 1465)
    end

    assert_equal('hello there Tanin!', client.greeting('Tanin'))  # make sure it works
    assert_equal(%w(127.0.0.1:1464 127.0.0.1:1465).sort, get_all_servers(client).sort)
  end
end
