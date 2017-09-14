require 'worker'
require 'msgpack/rpc'
require 'msgpack/rpc/transport/unix'

class RpcWorker < Worker
  attr_accessor :sock_path

  def get_client
    return RpcWorkerClient.new(self, sock_path)
  end

  private
  def sock_path
    @sock_path || "#{Dir.tmpdir}/#{@namespace}#{basename}.sock"
  end
  def worker_loop
    File.unlink(sock_path) rescue ""
    listener = MessagePack::RPC::UNIXServerTransport.new(sock_path)
    server = MessagePack::RPC::Server.new
    server.listen(listener, self)
    server.run
  end

  class RpcWorkerClient
    def initialize(worker, sock_path)
      @worker = worker
      @sock_path = sock_path

      base = RpcWorker.public_instance_methods
      methods = @worker.public_methods - base
      methods.each do |method|
        instance_eval <<-EOM
          def #{method}(*args)
            call(:#{method}, *args)
          end
        EOM
      end
    end

    def call(method, *args)
      transport = MessagePack::RPC::UNIXTransport.new
      client = MessagePack::RPC::Client.new(transport, @sock_path)
      return client.call(method, *args)
    end
  end
end

