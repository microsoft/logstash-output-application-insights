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
  class Blob

    attr_reader :instrumentation_key
    attr_reader :table_id
    attr_reader :storage_account_name
    attr_reader :container_name
    attr_reader :blob_name
    attr_reader :uploaded_events_count
    attr_reader :uploaded_bytesize
    attr_reader :oldest_event_time

    attr_reader :io_queue

    attr_reader :last_io_exception

    public

    @@closing = false

    def self.close
      @@closing = true
    end

    def self.stopped?
      @@closing
    end

    def initialize ( channel = nil, id = nil , no_queue = false )
      @configuration = Config.current
      @logger = @configuration[:logger]
      @storage_recovery = Storage_recovery.instance
      @notification_recovery = Notification_recovery.instance
      @max_tries = @configuration[:io_max_retries] + 1

      @uploaded_block_ids = [  ]
      @uploaded_block_numbers = [  ]
      @uploaded_bytesize = 0
      @uploaded_events_count = 0
      @sub_state = :none

      if channel
        @id = id
        @instrumentation_key = channel.instrumentation_key
        @table_id = channel.table_id
        @blob_max_delay = channel.blob_max_delay

        @event_format_ext = channel.event_format_ext

        unless no_queue

          @io_queue = Queue.new
          @timer = Timer.new
      
          # create a thread that handles the IO of the blob
          Thread.new do
            next_block = nil
            loop do
              block_to_upload = nil # release reference to resource for GC
              block_to_upload = next_block || @io_queue.pop
              next_block = nil

              if :trigger == @timer.state
                next_block = block_to_upload unless :wakeup == block_to_upload
                block_to_upload = :timeout
                to_commit = :commit

              elsif :close == block_to_upload
                to_commit = :commit

              # ignore :trigger as they are only to casue check timeout
              elsif :wakeup == block_to_upload # ignore :wakeup
                next

              else
                while @io_queue.length > 0
                  next_block = @io_queue.pop
                  next if :wakeup == next_block # ignore :wakeup
                  break if :close == next_block
                  break if blob_full?( next_block )
                  break unless block_to_upload.concat( next_block )
                  next_block = nil 
                end
              end

              unless to_commit
                @timer.set( block_to_upload.oldest_event_time + @blob_max_delay, nil ) {|object| @io_queue << :wakeup if 0 == @io_queue.length } if blob_empty?
                upload( block_to_upload )
                block_to_upload = nil # release reference to resource for GC
                to_commit = :commit if blob_full?
              end

              if to_commit
                commit unless @uploaded_block_ids.empty?
                to_commit = nil
                @uploaded_block_ids = [  ]
                @timer.cancel
                break if :close == block_to_upload
              end
            end
          end
        end

      end

    end

    # close blob. It will finish whatever was already on the queue, and if necessary commit
    # called on shutdown
    def close
      @io_queue << :close
    end

    def blob_full? ( next_block = nil )
      if next_block
        BLOB_MAX_BLOCKS < @uploaded_block_ids.length + 1 || @configuration[:blob_max_events] < @uploaded_events_count + next_block.events_count || @configuration[:blob_max_bytesize] < @uploaded_bytesize  + next_block.bytesize
      else
        BLOB_MAX_BLOCKS <= @uploaded_block_ids.length || @configuration[:blob_max_events] <= @uploaded_events_count || @configuration[:blob_max_bytesize] <= @uploaded_bytesize
      end
    end 

    def blob_empty?
      @uploaded_block_ids.empty?
    end

    def queue_empty?
      @io_queue.length == 0 if @io_queue
    end


    def queue_size
      @io_queue.length
    end

    def clear_state
      @action = nil
      @storage_account_name = nil
      @container_name = nil
      @blob_name = nil
      @uploaded_block_ids = [  ]
      @uploaded_block_numbers = [  ]
      @uploaded_events_count = 0
      @uploaded_bytesize = 0
      @oldest_event_time = nil
    end

    def table_entity_to_tuple( options = {} )
      [ options[:start_time.to_s] || Time.now.utc, options[:action.to_s], options[:instrumentation_key.to_s], options[:table_id.to_s], 
        options[:storage_account_name.to_s], options[:container_name.to_s], options[:blob_name.to_s], 
        eval( options[:uploaded_block_ids.to_s] ), eval( options[:uploaded_block_numbers.to_s] ), 
        options[:uploaded_events_count.to_s] || 0, options[:uploaded_bytesize.to_s] || 0, options[:oldest_event_time.to_s] || Time.now.utc,
        options[:event_format_ext.to_s], options[:blob_max_delay.to_s] || 0,
        options[:log_state.to_s].to_sym, (options[:sub_state.to_s] || :none).to_sym
      ]
    end

    def state_to_tuple
      [ @start_time || Time.now.utc, @action, @instrumentation_key, @table_id, 
        @storage_account_name, @container_name, @blob_name, 
        @uploaded_block_ids, @uploaded_block_numbers, 
        @uploaded_events_count, @uploaded_bytesize, @oldest_event_time,
        @event_format_ext, @blob_max_delay,
        @log_state, @sub_state
      ]
    end

    def tuple_to_state ( tuple )
      ( @start_time, @action, @instrumentation_key, @table_id, 
        @storage_account_name, @container_name, @blob_name, 
        @uploaded_block_ids, @uploaded_block_numbers, 
        @uploaded_events_count, @uploaded_bytesize, @oldest_event_time,
        @event_format_ext, @blob_max_delay,
        @log_state, @sub_state) = tuple
    end

    def state_to_table_entity
      { :start_time => @start_time, :instrumentation_key => @instrumentation_key, :table_id => @table_id, 
        :storage_account_name => @storage_account_name, :container_name => @container_name, :blob_name => @blob_name, 
        :uploaded_block_ids => @uploaded_block_ids.to_s, :uploaded_block_numbers => @uploaded_block_numbers.to_s, 
        :uploaded_events_count => @uploaded_events_count, :uploaded_bytesize => @uploaded_bytesize, :oldest_event_time => @oldest_event_time,
        :log_state => @log_state, :sub_state => @sub_state
      }
    end


    def notify_retry_later
      if :notify_failed_blob_not_accessible == @recovery
        @sub_state = @recovery
        @storage_recovery.recover_later( state_to_tuple, :notify, @storage_account_name )
      elsif :invalid_instrumentation_key == @recovery || :invalid_table_id == @recovery
        @sub_state = @recovery
        Channels.instance.channel( @instrumentation_key, @table_id ).failed_on_notify_retry_Q << state_to_tuple

      else
        if :notify_failed_blob_not_accessible == @sub_state
          @storage_recovery.recover_later( state_to_tuple, :notify, @storage_account_name )
        elsif :invalid_instrumentation_key == @sub_state || :invalid_table_id == @sub_state
          Channels.instance.channel( @instrumentation_key, @table_id ).failed_on_notify_retry_Q << state_to_tuple
        else
          @notification_recovery.recover_later( state_to_tuple )
        end
      end
    end

    # must return whether notification was successful or failed
    def notify ( tuple = nil )
      tuple_to_state( tuple ) if tuple
      @action = :notify
      @force_client = true # to enable get a client even if all storage_accounts marked dead
      @recoverable = [ :notify_failed_blob_not_accessible, :io_failure, :service_unavailable ]
      success = storage_io_block {
        set_blob_sas_url
        payload = create_payload
        @logger.debug { "notification payload: #{payload}" }
        @info = "#{@action.to_s} #{@storage_account_name}/#{@container_name}/#{@blob_name}, events: #{@uploaded_events_count}, size: #{@uploaded_bytesize}, blocks: #{@uploaded_block_numbers}, delay: #{Time.now.utc - @oldest_event_time}, blob_sas_url: #{@blob_sas_url}"

        # assume that exceptions can be raised due to this method:
        post_notification( @client.notifyClient, payload ) unless @configuration[:disable_notification]
        @log_state = :notified
      }
      if success
        Telemetry.instance.track_event { { :name => "notified", :properties => state_to_table_entity } }
        state_table_update
      else
        notify_retry_later
      end
      success
    end

    CREATE_EXIST_ERRORS = { :container => [ :create_container, :container_exist ], :table => [ :create_table, :table_exist ] }
    def create_exist_recovery( type, name = nil )
      prev_info = @info
      if CREATE_EXIST_ERRORS[type][0] == @recovery
        name ||= ( :table == type ? @configuration[:state_table_name] : @container_name )
        @info = "create #{type} #{@storage_account_name}/#{name}"

        # assume that exceptions can be raised due to this method:
        yield name
        @logger.info { "Successed to #{@info}" }
        @info = prev_info
      elsif CREATE_EXIST_ERRORS[type][1] == @recovery
        @logger.info { "Successed (already exist) to #{@info}" }
        @info = prev_info
      end
    end

    def create_table_exist_recovery
      create_exist_recovery( :table ) { |name| @client.tableClient.create_table( name ) }
    end

    def create_container_exist_recovery
      create_exist_recovery( :container ) { |name| @client.blobClient.create_container( name ) }
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

    def state_table_update ( tuple = nil )
      tuple_to_state( tuple ) if tuple
      if :uploading == @log_state
        state_table_delete
      elsif :committed == @log_state
        if state_table_insert && state_table_delete( nil, :uploading )
          State.instance.dec_pending_commits
          State.instance.inc_pending_notifications
          # this is not a recovery, it is actually enqueue to notify
          @notification_recovery.enqueue( state_to_tuple )
        end
      elsif :notified == @log_state
        if (!@configuration[:save_notified_blobs_records] || state_table_insert) && state_table_delete( nil, :committed ) 
          State.instance.dec_pending_notifications
        end
      end
    end


    # retturn tru on success
    def state_table_delete ( tuple = nil, state = nil )
      tuple_to_state( tuple ) if tuple
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

    def commit ( tuple = nil )
      tuple_to_state( tuple ) if tuple

      unless @uploaded_block_ids.empty?
        @action = :commit
        @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable ]
        success =  storage_io_block {
          @info = "#{@action.to_s} #{@storage_account_name}/#{@container_name}/#{@blob_name}, events: #{@uploaded_events_count}, size: #{@uploaded_bytesize}, blocks: #{@uploaded_block_numbers}, delay: #{Time.now.utc - @oldest_event_time}"
          # assume that exceptions can be raised due to this method:
          @client.blobClient.commit_blob_blocks( @container_name, @blob_name, @uploaded_block_ids ) unless @configuration[:disable_blob_upload]
          @log_state = :committed
        }
        if success
          # next stage
          state_table_update
        else
          @storage_recovery.recover_later( state_to_tuple, :commit, @storage_account_name )
        end
      end
    end


    def upload_retry_later
      unless @uploaded_block_ids.empty?
        info1 = "#{:commit} #{@storage_account_name}/#{@container_name}/#{@blob_name}, events: #{@uploaded_events_count}, size: #{@uploaded_bytesize}, blocks: #{@uploaded_block_numbers}, delay: #{Time.now.utc - @oldest_event_time}"
        @logger.error { "Pospone to #{info1} (; retry later, error: #{@last_io_exception.inspect}" }
        @storage_recovery.recover_later( state_to_tuple, :commit, @storage_account_name )
        @uploaded_block_ids = [  ]
      end
      unless :io_all_dead == @recovery
        raise UploadRetryError
      else 
        Channels.instance.channel( @instrumentation_key, @table_id ).failed_on_upload_retry_Q << @block_to_upload
        @block_to_upload = nil
      end
    end

    def upload ( block )
      @storage_account_name = nil if @uploaded_block_ids.empty?
      @block_to_upload = block
      block = nil # remove reference for GC
      exclude_storage_account_names = [  ]
      begin
        if @uploaded_block_ids.empty?
          @log_state = :uploading
          @uploaded_block_numbers = [  ]
          @uploaded_bytesize = 0
          @uploaded_events_count = 0
          @oldest_event_time = nil

          # remove record of previous upload that failed
          if @storage_account_name
            exclude_storage_account_names << @storage_account_name
            @storage_recovery.recover_later( state_to_tuple, :state_table_update, @storage_account_name )
          end
          set_conatainer_and_blob_names
          @storage_account_name = Clients.instance.get_random_active_storage( exclude_storage_account_names )
          unless @storage_account_name
            upload_recover.call( :io_all_dead, nil )
            return false
          end
          raise UploadRetryError unless state_table_insert
        end

        @action = :upload
        @block_info = "blocks: #{@block_to_upload.block_numbers}, events: #{@block_to_upload.events_count}, size: #{@block_to_upload.bytes.length}"
        @info = "#{@action} #{@storage_account_name}/#{@container_name}/#{@blob_name}, #{@block_info}, commitId: [\"#{100001 + @uploaded_block_ids.length}\"]"
        @recoverable = [ :invalid_storage_key, :invalid_storage_account, :io_failure, :service_unavailable, :container_exist, :create_container ]

        success = storage_io_block {
          create_container_exist_recovery
          block_id = "#{100001 + @uploaded_block_ids.length}"

          # assume that exceptions can be raised due to this method:
          @client.blobClient.put_blob_block( @container_name, @blob_name, block_id, @block_to_upload.bytes ) unless @configuration[:disable_blob_upload]

          # upload success
          first_block_in_blob = @uploaded_block_ids.empty?
          @uploaded_block_ids << [ block_id ]
          @uploaded_block_numbers.concat( @block_to_upload.block_numbers )
          @uploaded_bytesize += @block_to_upload.bytes.length
          @uploaded_events_count += @block_to_upload.events_count
          @oldest_event_time ||= @block_to_upload.oldest_event_time

          # release memory
          bytesize = @block_to_upload.bytesize
          @block_to_upload.dispose
          @block_to_upload = nil
          State.instance.inc_pending_commits if first_block_in_blob
          State.instance.dec_upload_bytesize( bytesize )
        }

        upload_retry_later unless success
      rescue UploadRetryError
        @recovery = nil
        retry
      end
    end

    def update_commited_or_uncommited_list( table_entity )
      tuple = table_entity_to_tuple( table_entity )

      tuple_to_state( tuple )
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
        type = ( blocks.empty? || 0 == @uploaded_bytesize ? :upload_empty : blocks[0].type )

        @log_state = :committed if :committed == type
        { type => state_to_tuple }
      else
        nil
      end
    end


    def << ( block )
      @io_queue << block
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

        puts " +++ recovery: #{@recovery}, reason: #{reason}"

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

      return [recovery, recovery] unless  recovery && @recoverable.include?( recovery )

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



    def set_conatainer_and_blob_names
      time_utc = Time.now.utc
      id = @id.to_s.rjust(4, "0")
      strtime = time_utc.strftime( "%F" )
      @container_name = "#{AZURE_STORAGE_CONTAINER_LOGSTASH_PREFIX}#{@configuration[:azure_storage_container_prefix]}-#{strtime}"

      strtime = time_utc.strftime( "%F-%H-%M-%S-%L" )
      # @blob_name = "#{@configuration[:azure_storage_blob_prefix]}_ikey-#{@instrumentation_key}_table-#{@table_id}_id-#{id}_#{strtime}.#{@event_format_ext}"
      @blob_name = "#{AZURE_STORAGE_BLOB_LOGSTASH_PREFIX}#{@configuration[:azure_storage_blob_prefix]}/ikey-#{@instrumentation_key}/table-#{@table_id}/#{strtime}_#{id}.#{@event_format_ext}"
    end


    def create_payload
      notification_hash = {
        :data => {
          :baseType => DATA_BASE_TYPE,
          :baseData => {
            :ver           => BASE_DATA_REQUIRED_VERSION,
            :blobSasUri    => @blob_sas_url.to_s,
            :sourceName    => @table_id,
            :sourceVersion => @configuration[:notification_version].to_s
          }
        }, 
        :ver  => @configuration[:notification_version], 
        :name => REQUEST_NAME,
        :time => Time.now.utc.iso8601,
        :iKey => @instrumentation_key
      }
      notification_hash.to_json
    end 


    def post_notification ( http_client, body )
      request = Azure::Core::Http::HttpRequest.new( :post, @configuration[:application_insights_endpoint], { :body => body, :client => http_client } )
      request.headers['Content-Type'] = 'application/json; charset=utf-8'
      request.headers['Accept'] = 'application/json'
      @logger.debug { "send notification : \n    endpoint: #{@configuration[:application_insights_endpoint]}\n    body : #{body}" }
      response = request.call
    end 


    def set_blob_sas_url
      blob_url ="https://#{@storage_account_name}.blob.core.windows.net/#{@container_name}/#{@blob_name}"
      options_and_constrains = {:permissions => "r", :resource => "b", :expiry => ( Time.now.utc + @configuration[:blob_access_expiry_time] ).iso8601 }
      @blob_sas_url = @client.storage_auth_sas.signed_uri( URI( blob_url ), options_and_constrains )
    end

  end
end
