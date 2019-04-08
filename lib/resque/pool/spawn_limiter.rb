module Resque
  class Pool
    class SpawnLimiter
      attr_reader :delay_until, :failed_count

      def initialize(delay_step:, delay_max:)
        @delay_step = delay_step
        @delay_max = delay_max
        reset
      end

      def delay_spawns
        @failed_count += 1

        # Exponential Backoff
        delay_secs = @delay_step ** @failed_count
        delay_secs = @delay_max if delay_secs > @delay_max
        @delay_until = Time.now.since(delay_secs)
      end

      def reset
        @failed_count = 0
        @delay_until = nil
      end

      def should_spawn?
        return true if @delay_until.nil?

        Time.now >= @delay_until
      end
    end
  end
end
