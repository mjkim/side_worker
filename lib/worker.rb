require 'tmpdir'
require 'logger'
def logger
  @logger ||= begin 
                logger = Logger.new(STDOUT)
                logger.level = 'WARN'
                logger
              end
end

class Worker
  attr_writer :namespace
  attr_reader :status

  module Status
    INITIALIZE = 0
    SLAVE = 1
    MASTER = 2
    SHUTDOWN = 3
  end

  def initialize(namespace=nil)
    @lock = Mutex.new
    @convar = ConditionVariable.new
    @namespace = namespace
    @status = Status::INITIALIZE
  end
  def basename
    self.class.name.gsub(/::/, '_')
  end

  def run(opts = {})
    if opts.fetch(:same_thread, false)
      run_here()
    elsif opts.fetch(:fork, false)
      logger.info "another thread for fork"
      @worker_thread = Thread.new {
        run_here(fork: true)
      }
    else
      logger.info "another thread"
      @worker_thread = Thread.new {
        run_here()
      }
    end
  end

  def run_here(opts = {})
    if @status != Status::INITIALIZE
      raise Exception.new("worker can not reuse")
    end
    @lock.synchronize do
      @status = Status::SLAVE
      wait_for_become_master
      logger.info "main thread wait start"
      @convar.wait(@lock)
      @status = Status::MASTER
      logger.info "now i'm master #{Thread.current.object_id}"
      if opts.fetch(:fork, false)
        begin
          pid = fork {
            logger.info "i'm worker fork thread #{Thread.current.object_id}"
            worker_loop
          }
          Process.wait(pid)
        ensure
          logger.info "kill worker proc"
          Process.kill(:SIGINT, pid)
        end
      else
        worker_loop
      end
    end
  end

  def worker_loop
    logger.info "worker loop start"
    begin
      loop { sleep 0.1 }
    rescue Interrupt => e
    end
  end
  
  def wait_for_become_master
    @monitor_thread = Thread.new do
      begin
        logger.info "wait thread wait lock #{Thread.current.object_id}"
        @lock.synchronize do
          logger.info "wait thread accuire lock"
          @lockfile = File.open(lockfile, File::RDWR|File::CREAT)
          logger.info "wait thread wait flock #{lockfile}"
          @lockfile.flock(File::LOCK_EX)
          logger.info "accuire flock"
          @convar.signal
        end
        loop { sleep 1 }
      rescue
        logger.error "error #{e.inspect}"
      ensure
        @lockfile.flock(File::LOCK_UN)
      end
    end
  end

  def shutdown
    @monitor_thread.terminate
    if @worker_thread
      @worker_thread.terminate
    end
    @status = Status::SHUTDOWN
  end

  def master?
    @status == Status::MASTER
  end

  def lockfile
    "#{Dir.tmpdir}/#{@namespace}#{basename}.lock"
  end
end
