require 'test_helper'
require 'rpc_worker'

class TestWorker < RpcWorker
  def test1(a)
    yield ({
      "result" => a,
      "thread"  => Thread.current.object_id,
    })
  end
end

class RpcTest < Minitest::Test
  def setup
    @worker1 = TestWorker.new
    @worker2 = TestWorker.new
    @worker3 = TestWorker.new
  end

  def test_worker_run
    @worker1.run
    sleep 0.1
    assert @worker1.master?
    @worker2.run
    sleep 0.1
    assert !@worker2.master?
    @worker3.run
    sleep 0.1
    assert !@worker3.master?

    master_client = @worker1.get_client
    master_ret = master_client.test1(3)
    assert master_ret["result"] == 3

    slave_client = @worker3.get_client
    slave_ret = master_client.test1(4)
    assert slave_ret["result"] == 4
    
    assert master_ret["thread"] == slave_ret["thread"]

    @worker1.shutdown
    sleep 0.1

    slave_client = @worker3.get_client
    slave_ret = master_client.test1(5)
    assert slave_ret["result"] == 5

    assert master_ret["thread"] != slave_ret["thread"]
  end
end
