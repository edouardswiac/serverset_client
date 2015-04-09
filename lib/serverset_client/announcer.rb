module ServersetClient
  class Announcer
    attr_reader :zk_host, :serverset_path
    def initialize(zk_host, serverset_path, options = {})
      @zk_host = zk_host
      @serverset_path = serverset_path
      @zk_options = options[:zk] || {}
      @creds = options[:creds]
    end

    def join(service, options = {})
      sequence = options.fetch(:sequence, true)
      node_path = sequence ? 'member_' : service.member_id
      zk.create("#{serverset_path}/#{node_path}", options.merge(:data => service.to_json, :sequence => sequence))
    rescue
      handle_zk_error
      raise
    end

    def leave(member_id)
      zk.delete("#{serverset_path}/#{member_id}")
    rescue
      handle_zk_error
      raise
    end

    def zk
      @zk ||= ZK.new(zk_host, @zk_options).tap do |zk|
        if @creds
          zk.send(:cnx).add_auth(:scheme => 'digest', :cert => @creds)
        end
      end
    end

    def handle_zk_error
      @zk.close if @zk
      @zk = nil
    end
  end
end
