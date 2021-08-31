# -*- encoding: utf-8 -*-
require 'resque'
require 'resque/worker'
require 'resque/pool/version'
require 'resque/pool/logging'
require 'resque/pool/pooled_worker'
require 'resque/pool/file_or_hash_loader'
require 'resque/pool/spawn_limiter'
require 'erb'
require 'fcntl'
require 'yaml'

module Resque
  class Pool
    SIG_QUEUE_MAX_SIZE = 5
    DEFAULT_WORKER_INTERVAL = 5
    QUEUE_SIGS = [ :QUIT, :INT, :TERM, :USR1, :USR2, :CONT, :HUP, :WINCH, ]
    CHUNK_SIZE = (16 * 1024)

    include Logging
    extend  Logging
    attr_reader :config
    attr_reader :config_loader
    attr_reader :workers
    attr_reader :spawn_limiter

    def initialize(config_loader=nil)
      init_config(config_loader)
      @workers = Hash.new { |workers, queues| workers[queues] = {} }
      @delay_spawn_limit = (ENV['DELAY_SPAWN_LIMIT'] || 10).to_i
      @spawn_limiter = Hash.new do |h, queues|
        h[queues] = SpawnLimiter.new(
          delay_step: @delay_spawn_limit,
          delay_max: (ENV['DELAY_SPAWN_MAX'] || 600).to_i,
        )
      end
      procline "(initialized)"
    end

    # Config: after_prefork {{{

    # The `after_prefork` hooks will be run in workers if you are using the
    # preforking master worker to save memory. Use these hooks to reload
    # database connections and so forth to ensure that they're not shared
    # among workers. The worker instance is passed as an argument to the block.
    #
    # Call with a block to set a hook.
    # Call with no arguments to return all registered hooks.
    #
    def self.after_prefork(&block)
      @after_prefork ||= []
      block ? (@after_prefork << block) : @after_prefork
    end

    # Sets the after_prefork proc, clearing all pre-existing hooks.
    # Warning: you probably don't want to clear out the other hooks.
    # You can use `Resque::Pool.after_prefork << my_hook` instead.
    #
    def self.after_prefork=(after_prefork)
      @after_prefork = [after_prefork]
    end

    def call_after_prefork!(worker)
      self.class.after_prefork.each do |hook|
        hook.call(worker)
      end
    end

    # }}}

    # Config: poll {{{

    # The `poll` hooks will be run in the pool master's main loop periodically.
    #
    # Call with a block to set a hook.
    # Call with no arguments to return all registered hooks.
    #
    def self.poll(&block)
      @poll ||= []
      block ? (@poll << block) : @poll
    end

    # Sets the poll proc, clearing all pre-existing hooks.
    # Warning: you probably don't want to clear out the other hooks.
    # You can use `Resque::Pool.poll << my_hook` instead.
    #
    def self.poll=(poll)
      @poll = [poll]
    end

    def call_poll!(pool)
      self.class.poll.each do |hook|
        hook.call(pool)
      end
    end

    # }}}

    # Config: register {{{

    # Use `register` to use different workers depending on the queue definitions.
    # Queue definitions that start with <kind>: will use these registrations to
    # figure out which worker class to instantiate.
    #
    def self.register(kind, worker_class)
      @registrations ||= {}

      # worker_class must include Resque::Pool::PooledWorker
      unless worker_class.include?(Resque::Pool::PooledWorker)
        worker_class.include Resque::Pool::PooledWorker
      end

      @registrations[kind.to_sym] = worker_class
    end

    def self.worker_class_for(kind)
      klass = @registrations&.dig(kind.to_sym)
      raise ArgumentError, "Registration missing for #{kind}" unless klass
      klass
    end

    # }}}

    # Config: class methods to start up the pool using the config loader {{{

    class << self; attr_accessor :config_loader, :app_name, :spawn_delay; end

    def self.app_name
      @app_name ||= File.basename(Dir.pwd)
    end

    def self.handle_winch?
      @handle_winch ||= false
    end
    def self.handle_winch=(bool)
      @handle_winch = bool
    end

    def self.single_process_group=(bool)
      ENV["RESQUE_SINGLE_PGRP"] = !!bool ? "YES" : "NO"
    end
    def self.single_process_group
      %w[yes y true t 1 okay sure please].include?(
        ENV["RESQUE_SINGLE_PGRP"].to_s.downcase
      )
    end

    def self.run
      if GC.respond_to?(:copy_on_write_friendly=)
        GC.copy_on_write_friendly = true
      end
      create_configured.start.join
    end

    def self.create_configured
      Resque::Pool.new(config_loader)
    end

    # }}}
    # Config: store loader and load config {{{

    def init_config(loader)
      case loader
      when String, Hash, nil
        @config_loader = FileOrHashLoader.new(loader)
      else
        @config_loader = loader
      end
      load_config
    end

    def load_config
      @config = config_loader.call(environment)
    end

    def reset_config
      config_loader.reset! if config_loader.respond_to?(:reset!)
      load_config
    end

    def environment
      if defined? RAILS_ENV
        RAILS_ENV
      elsif defined?(Rails) && Rails.respond_to?(:env)
        Rails.env
      else
        ENV['RACK_ENV'] || ENV['RAILS_ENV'] || ENV['RESQUE_ENV']
      end
    end

    # }}}

    # Sig handlers and self pipe management {{{

    def self_pipe; @self_pipe ||= [] end
    def sig_queue; @sig_queue ||= [] end
    def term_child; @term_child ||= ENV['TERM_CHILD'] end


    def init_self_pipe!
      self_pipe.each { |io| io.close rescue nil }
      self_pipe.replace(IO.pipe)
      self_pipe.each { |io| io.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC) }
    end

    def init_sig_handlers!
      QUEUE_SIGS.each { |sig| trap_deferred(sig) }
      trap(:CHLD)     { |_| awaken_master }
    end

    def set_master
      @master_pid = Process.pid
    end

    def is_master?
      @master_pid == Process.pid
    end

    def awaken_master
      return unless is_master?
      begin
        self_pipe.last.write_nonblock('.') # wakeup master process from select
      rescue Errno::EAGAIN, Errno::EINTR
        # pipe is full, master should wake up anyways
        retry
      end
    end

    class QuitNowException < Exception; end
    # defer a signal for later processing in #join (master process)
    def trap_deferred(signal)
      trap(signal) do |sig_nr|
        next unless is_master?
        if @waiting_for_reaper && [:INT, :TERM].include?(signal)
          log "Recieved #{signal}: short circuiting QUIT waitpid"
          raise QuitNowException
        end
        if sig_queue.size < SIG_QUEUE_MAX_SIZE
          sig_queue << signal
          awaken_master
        else
          log "ignoring SIG#{signal}, queue=#{sig_queue.inspect}"
        end
      end
    end

    def reset_sig_handlers!
      trap(:CHLD, "DEFAULT")
      QUEUE_SIGS.each {|sig| trap(sig, "DEFAULT") }
    end

    def handle_sig_queue!
      case signal = sig_queue.shift
      when :USR1, :USR2, :CONT
        log "#{signal}: sending to all workers"
        signal_all_workers(signal)
      when :HUP
        log "HUP: reset configuration and reload logfiles"
        reset_config
        Logging.reopen_logs!
        log "HUP: gracefully shutdown old children (which have old logfiles open)"
        if term_child
          signal_all_workers(:TERM)
        else
          signal_all_workers(:QUIT)
        end
        log "HUP: new children will inherit new logfiles"
        maintain_worker_count
      when :WINCH
        if self.class.handle_winch?
          log "WINCH: gracefully stopping all workers"
          @config = {}
          maintain_worker_count
        end
      when :QUIT
        if term_child
          shutdown_everything_now!(signal)
        else
          graceful_worker_shutdown_and_wait!(signal)
        end
      when :INT
        graceful_worker_shutdown!(signal)
      when :TERM
        case self.class.term_behavior
        when "graceful_worker_shutdown_and_wait"
          graceful_worker_shutdown_and_wait!(signal)
        when "graceful_worker_shutdown"
          graceful_worker_shutdown!(signal)
        when "term_and_wait"
          term_and_wait!(signal)
        else
          shutdown_everything_now!(signal)
        end
      end
    end

    class << self
      attr_accessor :term_behavior
    end

    def graceful_worker_shutdown_and_wait!(signal)
      log "#{signal}: graceful shutdown, waiting for children"
      signal_all_workers(:USR2) # Stop all workers from picking up new jobs
      if term_child
        signal_all_workers(:TERM)
      else
        signal_all_workers(:QUIT)
      end
      reap_all_workers(0) # will hang until all workers are shutdown
      :break
    end

    def graceful_worker_shutdown!(signal)
      log "#{signal}: immediate shutdown (graceful worker shutdown)"
      signal_all_workers(:USR2) # Stop all workers from picking up new jobs
      if term_child
        signal_all_workers(:TERM)
      else
        signal_all_workers(:QUIT)
      end
      :break
    end

    def shutdown_everything_now!(signal)
      log "#{signal}: immediate shutdown (and immediate worker shutdown)"
      signal_all_workers(:USR2) # Stop all workers from picking up new jobs
      if term_child
        signal_all_workers(:QUIT)
      else
        signal_all_workers(:TERM)
      end
      :break
    end

    def term_and_wait!(signal)
      log "#{signal}: term_and_wait, waiting for children"
      signal_all_workers(:USR2) # Stop all workers from picking up new jobs
      signal_all_workers(:TERM)
      reap_all_workers(0) # will hang until all workers are shutdown
      :break
    end

    # }}}
    # start, join, and master sleep {{{

    def start
      procline("(starting)")
      set_master
      init_self_pipe!
      init_sig_handlers!
      maintain_worker_count
      procline("(started)")
      log "started manager"
      report_worker_pool_pids
      self
    end

    def report_worker_pool_pids
      if workers.empty?
        log "Pool is empty"
      else
        log "Pool contains worker PIDs: #{all_pids.inspect}"
      end
    end

    def join
      loop do
        reap_all_workers
        poll
        break if handle_sig_queue! == :break
        if sig_queue.empty?
          master_sleep
          load_config
          maintain_worker_count
        end
        procline("managing #{all_pids.inspect}")
      end
      procline("(shutting down)")
      #stop # gracefully shutdown all workers on our way out
      log "manager finished"
      #unlink_pid_safe(pid) if pid
    end

    def poll
      call_poll!(self)
    end

    def master_sleep
      begin
        ready = IO.select([self_pipe.first], nil, nil, 1) or return
        ready.first && ready.first.first or return
        loop { self_pipe.first.read_nonblock(CHUNK_SIZE) }
      rescue Errno::EAGAIN, Errno::EINTR
      end
    end

    # }}}
    # worker process management {{{

    def reap_all_workers(waitpid_flags=Process::WNOHANG)
      @waiting_for_reaper = waitpid_flags == 0
      begin
        reaped = Hash.new { |h, queues| h[queues] = [] }
        loop do
          # -1, wait for any child process
          wpid, status = Process.waitpid2(-1, waitpid_flags)
          break unless wpid

          if worker = delete_worker(wpid)
            log "Reaped resque worker[#{status.pid}] (status: #{status.exitstatus}) queues: #{worker.queues.join(",")}"
            reaped[worker.queue_definition] << worker.spawned_at
          else
            # this died before it could be killed, so it's not going to have any extra info
            log "Tried to reap worker [#{status.pid}], but it had already died. (status: #{status.exitstatus})"
          end
        end

        # Check if we are having trouble starting
        now = Time.now
        reaped.each do |queues, starts|
          oldest = starts.min
          if !oldest.nil? && (now - oldest) < @delay_spawn_limit
            spawn_limiter[queues].delay_spawns
          else
            spawn_limiter.delete(queues)
          end
        end
      rescue Errno::ECHILD, QuitNowException
      end
    end

    # TODO: close any file descriptors connected to worker, if any
    def delete_worker(pid)
      worker = nil
      workers.detect do |queues, pid_to_worker|
        worker = pid_to_worker.delete(pid)
      end
      worker
    end

    def all_pids
      workers.map {|q,workers| workers.keys }.flatten
    end

    def signal_all_workers(signal)
      log "Sending #{signal} to all workers"
      all_pids.each do |pid|
        Process.kill signal, pid
      end
    end

    # }}}
    # ???: maintain_worker_count, all_known_queues {{{

    def maintain_worker_count
      all_known_queues.each do |queues|
        delta = worker_delta_for(queues)
        spawn_missing_workers_for(queues) if delta > 0
        quit_excess_workers_for(queues)   if delta < 0
      end
    end

    def all_known_queues
      config.keys | workers.keys
    end

    # }}}
    # methods that operate on a single grouping of queues {{{
    # perhaps this means a class is waiting to be extracted

    def spawn_missing_workers_for(queues)
      worker_delta_for(queues).times do |nr|
        spawn_worker!(queues)
        sleep Resque::Pool.spawn_delay if Resque::Pool.spawn_delay
      end
    end

    def quit_excess_workers_for(queues)
      delta = -worker_delta_for(queues)
      pids_for(queues)[0...delta].each do |pid|
        Process.kill("QUIT", pid)
      end
    end

    def worker_delta_for(queues)
      delta = config.fetch(queues, 0) - workers.fetch(queues, []).size

      # Only allow downwards deltas while spawn is limited
      ql = spawn_limiter[queues]
      if delta > 0 && !ql.should_spawn?
        puts "Delaying spawn until #{ql.delay_until} (failed_count=#{ql.failed_count}) for #{queues}"
        0
      else
        delta
      end
    end

    def pids_for(queues)
      workers[queues].keys
    end

    def spawn_worker!(queues)
      worker = create_worker(queues)
      pid = fork do
        Process.setpgrp unless Resque::Pool.single_process_group
        worker.worker_parent_pid = Process.pid
        log_worker "Starting worker #{worker}"
        reset_sig_handlers!
        #self_pipe.each {|io| io.close }
        call_after_prefork!(worker)
        worker.work(ENV['INTERVAL'] || DEFAULT_WORKER_INTERVAL) # interval, will block
      end
      workers[queues][pid] = worker
    end

    def create_worker(queues)
      worker = new_worker(queues)
      worker.queue_definition = queues
      worker.spawned_at = Time.now
      worker.pool_master_pid = Process.pid
      worker.term_timeout = ENV['RESQUE_TERM_TIMEOUT'] || 4.0
      worker.term_child = ENV['TERM_CHILD']
      if worker.respond_to?(:run_at_exit_hooks=)
        # resque doesn't support this until 1.24, but we support 1.22
        worker.run_at_exit_hooks = ENV['RUN_AT_EXIT_HOOKS'] || false
      end
      if ENV['LOGGING'] || ENV['VERBOSE']
        worker.verbose = ENV['LOGGING'] || ENV['VERBOSE']
      end
      if ENV['VVERBOSE']
        worker.very_verbose = ENV['VVERBOSE']
      end
      worker
    end

    def new_worker(queues)
      cidx = queues.index(':')
      if cidx
        kind = queues[0..cidx-1].to_sym

        klass = self.class.worker_class_for(kind)
        queues = queues[cidx+1..queues.length]
      else
        klass = ::Resque::Worker
      end

      queues = queues.to_s.split(',')
      klass.new(*queues)
    end
    # }}}

  end
end
