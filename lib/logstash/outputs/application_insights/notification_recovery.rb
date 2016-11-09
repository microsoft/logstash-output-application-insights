# encoding: utf-8

# ----------------------------------------------------------------------------------
# Logstash Output Application Insights
#
# Copyright (c) Microsoft Corporation
#
# All rights reserved. 
#
# Licensed under the Apache License, Version 2.0 (the License); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 
#
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
#
# See the Apache Version 2.0 License for specific language governing 
# permissions and limitations under the License.
# ----------------------------------------------------------------------------------
class LogStash::Outputs::Application_insights
  class Notification_recovery

    public

    def initialize
      configuration = Config.current
      @resurrect_delay = configuration[:resurrect_delay]
      @queue = Queue.new

      @closing = nil
      @thread = nil
    end

    def start
      @test_notification = Test_notification.new
      @thread = recovery_thread
    end

    def recover_later ( tuple )
      @notification_state_on = false
      if stopped?
        @state ||= State.instance
        @state.dec_pending_notifications
        @shutdown ||= Shutdown.instance
        @shutdown.display_msg("!!! notification won't recover in this session due to shutdown")
      else
        @queue << tuple
      end
    end

    def enqueue ( tuple )
      @queue << tuple
    end

    def close
      @closing = true
      # @thread.join
    end

    private

    def stopped?
      @closing
    end

    def recovery_thread
      Thread.new do
        counter = Concurrent::AtomicFixnum.new(0)

        loop do
          tuple = @queue.pop
          Stud.stoppable_sleep(Float::INFINITY, 1) { ( state_on? || stopped? ) && 10 > counter.value }

          if stopped? && !state_on?
            recover_later( tuple )
          else
            counter.increment
            Thread.new( counter, tuple ) do |counter, tuple|
              Notification.new( tuple ).notify
              counter.decrement
            end
          end
          tuple = nil # release for GC
        end
      end
    end

    def state_on?
      return @notification_state_on if @notification_state_on
      @notification_state_on = @test_notification.test
      return @notification_state_on if @notification_state_on
      sleep( @resurrect_delay )
      @notification_state_on
    end

    public

    @@instance = Notification_recovery.new

    def self.instance
      @@instance
    end

    private_class_method :new
  end
end
