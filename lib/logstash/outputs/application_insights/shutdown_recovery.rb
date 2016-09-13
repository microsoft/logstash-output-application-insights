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
  class Shutdown_recovery

    public

    def initialize
      configuration = Config.current
      @logger = configuration[:logger]
      @storage_account_name_key = configuration[:storage_account_name_key]
      @partition_key_prefix =configuration[:azure_storage_blob_prefix].gsub( "/", "" )

      @storage_recovery = Storage_recovery.instance
      @notification_recovery = Notification_recovery.instance

      @closing = nil
      @threads = []
    end

    def start
      @storage_account_name_key.each do |storage_account_name, storage_account_keys|
        @threads << recovery_thread( storage_account_name, :uploading)
        @threads << recovery_thread( storage_account_name, :committed)
      end
    end

    def close
      @closing = true
      @threads.each do |thread|
        thread.join
      end
    end

    private

    def stopped?
      @closing
    end

    def recovery_thread( storage_account_name, state )
      Thread.new( storage_account_name, state ) do |storage_account_name, state|
        
        blob = Blob.new

        committed_tuples = [  ]
        uncommitted_tuples = [  ]
        upload_empty_tuples = [  ]
        token = nil
        finished = false
        filter = "#{:PartitionKey} eq '#{@partition_key_prefix}-#{state}'"

        # should exit thread after fetching data from table, and submit recovery, the loop is only for case of failure
        until finished || stopped? do
          entities = blob.state_table_query( storage_account_name, filter, token )
          if entities
            token = entities.continuation_token

            if :committed == state
              entities.each do |entity|
                State.instance.inc_pending_notifications
                tuple = blob.table_entity_to_tuple( entity.properties )
                @notification_recovery.enqueue( tuple )
              end

            elsif :uploading == state
              # first tuples are collected, before send to queues, to make sure blob states don't change in between
              entities.each do |entity|
                typed_tuple = nil
                until typed_tuple || stopped?
                  typed_tuple = blob.update_commited_or_uncommited_list( entity.properties )
                  Stud.stoppable_sleep(60, 1) { stopped? } unless typed_tuple
                end

                next if stopped?

                if typed_tuple[:committed]
                  committed_tuples << typed_tuple[:committed]
                elsif typed_tuple[:uncommitted]
                  uncommitted_tuples << typed_tuple[:uncommitted]
                else
                  upload_empty_tuples << typed_tuple[:upload_empty]
                end
              end
            end

            next if token

            committed_tuples.each do |tuple|
              State.instance.inc_pending_commits
              @storage_recovery.recover_later( tuple, :state_table_update, storage_account_name )
            end

            uncommitted_tuples.each do |tuple|
              State.instance.inc_pending_commits
              @storage_recovery.recover_later( tuple, :commit, storage_account_name )
            end

            upload_empty_tuples.each do |tuple|
              @storage_recovery.recover_later( tuple, :state_table_update, storage_account_name )
            end

            finished = true
          else
            Stud.stoppable_sleep(60, 1) { stopped? }
          end
        end
        @logger.info { "exit table recovery thread, storage: #{storage_account_name}, state: #{state}, entities: #{entities ? entities.length : nil}" }
      end
    end

    public

    @@instance = Shutdown_recovery.new

    def self.instance
      @@instance
    end

    private_class_method :new
  end
end
