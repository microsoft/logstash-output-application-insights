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
      @logger = configuration[:logger]
      @storage_account_name_key = configuration[:storage_account_name_key]
      @queue = Queue.new

      @closing = nil
      @thread = nil
    end

    def start
      @thread = recovery_thread
    end

    def recover_later ( tuple )
      @notification_state_on = false
      @queue << tuple
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

    def init_queues ( storage_account_name_key, queues )
      storage_account_name_key.each do |storage_account_name, storage_account_keys|
        queues.each_key do |action|
          queues[action][storage_account_name] = Queue.new
        end
      end
    end

    def recovery_thread
      Thread.new do
        blob = Blob.new
        counter = Concurrent::AtomicFixnum.new(0)

        loop do
          tuple = @queue.pop
          Stud.stoppable_sleep(Float::INFINITY, 1) { state_on?( blob ) && 10 > counter.value }

          counter.increment
          Thread.new( counter, tuple ) do |counter, tuple|
            Blob.new.send( :notify, tuple )
            counter.decrement
          end
          tuple = nil # release for GC
        end
      end
    end

    def state_on? ( blob )
      @notification_state_on ||= blob.test_notification( @storage_account_name_key[0][0] )
    end

    public

    @@instance = Notification_recovery.new

    def self.instance
      @@instance
    end

    private_class_method :new
  end
end
