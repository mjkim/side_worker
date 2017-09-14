require 'test_helper'
require 'worker'

class WorkerTest < Minitest::Test
  def setup
    @worker = Worker.new
  end

  def test_worker_run
    @worker.run
    sleep 0.1
    assert @worker.master?

    worker2 = Worker.new
    worker2.run

    sleep 0.1
    assert !worker2.master?

    @worker.shutdown
    sleep 0.1
    assert !@worker.master?
    assert worker2.master?
    worker2.shutdown
    sleep 0.1
  end

  def test_worker_fork_run
    @worker.run(fork: true)
    sleep 0.1
    assert @worker.master?

    worker2 = Worker.new
    worker2.run(fork: true)

    sleep 0.1
    assert !worker2.master?

    sleep 0.1
    @worker.shutdown
    sleep 0.1
    assert !@worker.master?
    assert worker2.master?
    worker2.shutdown
    sleep 0.1
  end
end
