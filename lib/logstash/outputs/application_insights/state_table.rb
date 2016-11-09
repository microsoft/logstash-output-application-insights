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
  class State_table < Blob

    def initialize( tuple )
      # super first parameter must be nil. blob first parameter is channel, otherwise it will pass storage_account_name as channel
      super( nil )
      tuple_to_state( tuple ) if tuple
    end

    # return true on success
    def state_table_insert
      @action = :state_table_insert
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable, :table_exist, :create_table, :table_busy, :entity_exist ]
      @info  = "#{@action} #{@log_state} #{@storage_account_name}/#{@container_name}/#{@blob_name}"
      success =  storage_io_block {
        create_table_exist_recovery
        if :entity_exist == @recovery
          raise NotRecoverableError if :uploading == @log_state
        else
          entity_values = state_to_table_entity
          entity_values[:PartitionKey] = "#{@configuration[:partition_key_prefix]}-#{@log_state}"
          entity_values[:RowKey] = @blob_name.gsub("/","_")
          @client.tableClient.insert_entity( @configuration[:state_table_name], entity_values )
        end
      }
      @storage_recovery.recover_later( state_to_tuple, :state_table_update, @storage_account_name )  unless success || :uploading == @log_state
      success
    end


    def state_table_update
      if :uploading == @log_state
        state_table_delete
      elsif :committed == @log_state
        if state_table_insert && state_table_delete( :uploading )
          State.instance.dec_pending_commits
          State.instance.inc_pending_notifications
          # this is not a recovery, it is actually enqueue to notify
          @notification_recovery.enqueue( state_to_tuple )
        end
      elsif :notified == @log_state
        if (!@configuration[:save_notified_blobs_records] || state_table_insert) && state_table_delete( :committed ) 
          State.instance.dec_pending_notifications
        end
      end
    end


    # return tru on success
    def state_table_delete ( state = nil )
      state ||= @log_state
      @action = :state_table_delete
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable, :table_exist, :create_table, :table_busy, :create_resource ]
      @info  = "#{@action} #{state} #{@storage_account_name}/#{@container_name}/#{@blob_name}"

      success =  storage_io_block {
        create_table_exist_recovery
        if :create_resource == @recovery
          @logger.info { "Note: delete entity failed, already deleted, #{@info}, state: #{state}, log_state: #{@log_state}" }
        else
          @client.tableClient.delete_entity( @configuration[:state_table_name], "#{@configuration[:partition_key_prefix]}-#{state}", @blob_name.gsub( "/", "_" ) )
        end
      }
      @storage_recovery.recover_later( state_to_tuple, :state_table_update, @storage_account_name ) unless success
      success
    end


    # return entities
    def state_table_query ( storage_account_name, filter , token )
      @storage_account_name = storage_account_name

      @action = :state_table_query
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable, :table_exist, :create_table, :table_busy ]
      @info  = "#{@action} #{@storage_account_name}/#{@configuration[:state_table_name]}"

      entities = nil
      success =  storage_io_block {
        create_table_exist_recovery
        options = { :filter => filter }
        options[:continuation_token] = token if token
        entities = @client.tableClient.query_entities( @configuration[:state_table_name], options )
      }
      entities
    end

  end
end
