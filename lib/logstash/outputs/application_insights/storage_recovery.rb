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
  class Storage_recovery

    public

    def initialize
      configuration = Config.current
      @logger = configuration[:logger]
      @storage_account_name_key = configuration[:storage_account_name_key]

      @queues = { :commit => {}, :notify => {}, :state_table_update => {} }
      init_queues( @storage_account_name_key, @queues )

      @closing = nil
      @threads = []
    end

    def start
      @storage_account_name_key.each do |storage_account_name, storage_account_keys|
        # a threads, per storage  account name
        @queues.each_key do |action|
          @threads << recovery_thread( storage_account_name, action )
        end
      end
    end

    def recover_later ( tuple, action , storage_account_name )
      if stopped?
        if :commit == action
          @state ||= State.instance
          @state.dec_pending_commits
          @shutdown ||= Shutdown.instance
          @shutdown.display_msg("!!! commit won't recover in this session due to shutdown")
        end
      else
        @queues[action][storage_account_name] << tuple
      end
    end

    def close
      @closing = true
      # @threads.each do |thread|
      #   thread.join
      # end
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

    def recovery_thread( storage_account_name, action )
      # a threads, per storage  account name, that retries actions on storage
      Thread.new( storage_account_name, action ) do |storage_account_name, action|
        counter = Concurrent::AtomicFixnum.new(0)
        queue = @queues[action][storage_account_name]
        loop do
          tuple = queue.pop
          Stud.stoppable_sleep(Float::INFINITY, 1) { ( state_on?( storage_account_name )  || stopped? ) && 10 > counter.value }

          if stopped? && !state_on?( storage_account_name )
            recover_later( tuple, action, storage_account_name )
          else
            counter.increment
            Thread.new( action, counter, tuple ) do |action, counter, tuple|
              if :notify == action
                Notification.new( tuple ).notify
              elsif :commit == action
                Upload_pipe.new( nil, nil, tuple ).commit
              elsif :state_table_update == action
                Blob.new( tuple ).state_table_update
              end
              counter.decrement
            end
          end
          tuple = nil # release for GC
        end
      end
    end

    def state_on? ( storage_account_name )
      Clients.instance.storage_account_state_on?( storage_account_name )
    end

    public

    @@instance = Storage_recovery.new

    def self.instance
      @@instance
    end

    private_class_method :new
  end
end
