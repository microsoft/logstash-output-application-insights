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
  class Blob < Context

    attr_reader :last_io_exception

    public

    @@closing = false

    def self.close
      @@closing = true
    end

    def self.stopped?
      @@closing
    end

    def initialize ( tuple = nil )
      @configuration = Config.current
      @logger = @configuration[:logger]
      @storage_recovery = Storage_recovery.instance
      @notification_recovery = Notification_recovery.instance
      @max_tries = @configuration[:io_max_retries] + 1

      super( tuple )
    end

    CREATE_EXIST_ERRORS = { :container => [ :create_container, :container_exist ], :table => [ :create_table, :table_exist ] }
    def create_exist_recovery( type, name )
      if CREATE_EXIST_ERRORS[type][0] == @recovery
        @prev_info = @info
        @info = "create #{type} #{@storage_account_name}/#{name}"

        # assume that exceptions can be raised due to this method:
        yield name
        @logger.info { "Successed to #{@info}" }
        @info = @prev_info
      elsif CREATE_EXIST_ERRORS[type][1] == @recovery
        @logger.info { "Successed (already exist) to #{@info}" }
        @info = @prev_info
      end
    end

    def create_table_exist_recovery
      create_exist_recovery( :table, @configuration[:state_table_name] ) { |name| @client.tableClient.create_table( name ) }
    end

    def create_container_exist_recovery
      create_exist_recovery( :container, @container_name ) { |name| @client.blobClient.create_container( name ) }
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
          entity_values = context_to_table_entity
          entity_values[:PartitionKey] = "#{@configuration[:partition_key_prefix]}-#{@log_state}"
          entity_values[:RowKey] = @blob_name.gsub("/","_")
          @client.tableClient.insert_entity( @configuration[:state_table_name], entity_values )
        end
      }
      @storage_recovery.recover_later( context_to_tuple, :state_table_update, @storage_account_name )  unless success || :uploading == @log_state
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
          @notification_recovery.enqueue( context_to_tuple )
        end
      elsif :notified == @log_state
        if (!@configuration[:save_notified_blobs_records] || state_table_insert) && state_table_delete( :committed ) 
          State.instance.dec_pending_notifications
        end
      end
    end


    # return true on success
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
      @storage_recovery.recover_later( context_to_tuple, :state_table_update, @storage_account_name ) unless success
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


    def update_commited_or_uncommited_list
      @action = :list_blob_blocks
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable, :container_exist, :create_container, :create_blob ]
      list_blob_blocks = nil
      success =  storage_io_block {
        @info = "#{@action} #{@storage_account_name}/#{@container_name}/#{@blob_name}"

        create_container_exist_recovery
        if :create_blob == @recovery
          list_blob_blocks = { :uncommitted => [  ], :committed => [  ] }
        else
          list_blob_blocks = @client.blobClient.list_blob_blocks( @container_name, @blob_name, { :blocklist_type => :all } ) unless :create_blob == @recovery
        end
      }

      if list_blob_blocks
        blocks = ( list_blob_blocks[:uncommitted].empty? ? list_blob_blocks[:committed] : list_blob_blocks[:uncommitted] )
        blocks.each do |block|
          @uploaded_block_ids << [ block.name ]
          @uploaded_bytesize += block.size
        end
        if 0 < @file_size  &&  @uploaded_bytesize != @file_size
          type = :upload_empty
        else
          type = ( blocks.empty? || 0 == @uploaded_bytesize ? :upload_empty : blocks[0].type )
        end

        @log_state = :committed if :committed == type
        { type => context_to_tuple }
      else
        nil
      end
    end


    private


    def storage_io_block
      @recovery = nil
      @try_count = 1

      begin
        @client ||= Client.new( @storage_account_name, @force_client )
        yield
        disabled = :notify == @action ? @configuration[:disable_notification] : @configuration[:disable_blob_upload]
        @logger.info { "Successed to #{disabled ? 'DISABLED ' : ''}#{@info}" }
        true

      rescue TypeError
        raise

      rescue StandardError => e
        @last_io_exception = e
        @recovery, reason = recover_retry?( e )
        retry if @recovery || reason.nil?


        @recovery = reason
        @logger.error { "Failed to #{@info} ; retry later, error: #{e.inspect}" }
        false

      ensure
        @client = @client.dispose if @client
      end
    end


    def error_to_sym ( e )
      if e.is_a?( Azure::Core::Http::HTTPError )
        if 404 == e.status_code
          if "ContainerNotFound" == e.type
            :create_container

          elsif "TableNotFound" == e.type
            :create_table

          elsif "BlobNotFound" == e.type
            :create_blob
          
          elsif "ResourceNotFound" == e.type
            :create_resource

          else
            :create_resource
          end

        elsif 409 == e.status_code 
          if "ContainerAlreadyExists" == e.type
            :container_exist

          elsif "BlobAlreadyExists" == e.type
            :blob_exist

          elsif "TableAlreadyExists" == e.type
            :table_exist

          elsif "TableBeingDeleted" == e.type
            :table_busy

          elsif "EntityAlreadyExists" == e.type
            :entity_exist

          else
            :http_unknown
          end

        elsif 403 == e.status_code 
          if "AuthenticationFailed" == e.type
            :invalid_storage_key

          elsif "Unknown" == e.type && e.description.include?("Blob does not exist or not accessible.")
            :notify_failed_blob_not_accessible

          else
            :access_denied
          end

        elsif 400 == e.status_code  && "Unknown" == e.type && e.description.include?("Invalid instrumentation key")
          :invalid_instrumentation_key

        elsif 500 == e.status_code  && "Unknown" == e.type && e.description.include?("Processing error")
          :notification_process_down

        elsif 503 == e.status_code
          :service_unavailable

        else
          :http_unknown
        end

      # communication error
      elsif e.is_a?( Faraday::ClientError )
        :io_failure

      # communication error
      elsif e.is_a?( IOError )
        :io_failure

      # all storage accounts are dead, couldn't get client (internal exception)
      elsif e.is_a?( StorageAccountsOffError )
        :io_all_dead

      # all storage accounts are dead, couldn't get client (internal exception)
      elsif e.is_a?( NotRecoverableError )
        :not_recoverable

      elsif e.is_a?( NameError ) && e.message.include?( "uninitialized constant Azure::Core::Auth::Signer::OpenSSL" )
        :init_error

      elsif e.is_a?( NameError ) && e.message.include?( "uninitialized constant Azure::Storage::Auth::SharedAccessSignature" )
        :init_error

      else
        :unknown
      end
    end


    def recover_retry? ( e )
      recovery = error_to_sym( e )
      if :init_error == recovery
        @client = @client.dispose if @client
        sleep( 1 )
        recovery = nil

      elsif :http_unknown == recovery || :unknown == recovery
        puts "\n>>>> UNKNOWN error - #{e.inspect} <<<<\n"
        raise e if  @configuration[:stop_on_unknown_io_errors]
      end

      return [nil, recovery] unless  recovery && @recoverable.include?( recovery )

      case recovery
      when :container_exist, :table_exist, :entity_exist, :create_container, :create_table
        # ignore log error
        # @logger.error { "Failed to #{@info} ;( recovery: continue, error: #{e.inspect}" }

      when :invalid_storage_key, :notify_failed_blob_not_accessible
        if @client.switch_storage_account_key!
          @logger.error { "Failed to #{@info} ;( recovery: switched to secondary storage key, error: #{e.inspect}" }
        else
          @client = @client.dispose( :auth_to_storage_failed ) if @client && :invalid_storage_key == recovery 
          return [nil, recovery]
        end

      when :table_busy
        @client = @client.dispose if @client
        sleep( @configuration[:io_retry_delay] )
        @logger.error { "Failed to #{@info} ;( recovery: retry, error: #{e.inspect}" }

      when :io_failure, :service_unavailable, :notification_process_down
        if @try_count < @max_tries
          @client = @client.dispose if @client
          sleep( @configuration[:io_retry_delay] )
          @logger.error { "Failed to #{@info} ;( recovery: retry, try #{@try_count} / #{@max_tries}, error: #{e.inspect}" }
          @try_count += 1
        else
          if :io_failure == recovery || ( :service_unavailable == recovery && :notify != @action )
            @client = @client.dispose( :io_to_storage_failed ) if @client
          end
          return [nil, recovery]
        end

      when :invalid_instrumentation_key, :invalid_table_id
        if :notify == @action # only for notify, not for test endpoint
          if @try_count < @max_tries
            @client = @client.dispose if @client
            sleep( @configuration[:io_retry_delay] )
            @logger.error { "Failed to #{@info} ;( recovery: retry, try #{@try_count} / #{@max_tries}, error: #{e.inspect}" }
            @try_count += 1
          else
            if :invalid_instrumentation_key == recovery
              Channels.instance.mark_invalid_instrumentation_key( @instrumentation_key )
            elsif :invalid_table_id == recovery
              Channels.instance.mark_invalid_table_id( @table_id )
            end
            return [nil, recovery]
          end
        end
      end
      [recovery, recovery]
    end



  end
end
