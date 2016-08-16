# encoding: utf-8
#-------------------------------------------------------------------------
# # Copyright (c) Microsoft and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#--------------------------------------------------------------------------

class LogStash::Outputs::Application_insights
  class Blob

    attr_reader :intrumentation_key
    attr_reader :table_id
    attr_reader :storage_account_name
    attr_reader :container_name
    attr_reader :blob_name
    attr_reader :uploaded_events_count
    attr_reader :uploaded_bytesize
    attr_reader :oldest_event_time

    attr_reader :io_queue

    public

    def self.config ( configuration )
      @@configuration = configuration

      @@logger = configuration[:logger]
      @@io_retry_delay = configuration[:io_retry_delay]
      @@io_max_retries = configuration[:io_max_retries]
      @@blob_max_bytesize = configuration[:blob_max_bytesize]
      @@blob_max_events = configuration[:blob_max_events]
      @@state_table_name = "#{configuration[:azure_storage_table_prefix]}#{STATE_TABLE_NAME}"
      @@save_notified_blobs_records = configuration[:save_notified_blobs_records]

      @@closing = false

      # queues, per storage_account_name, for failed blob commit, will continue to try resending
      @@failed_on_commit_retry_Qs = {}
      launch_storage_recovery_threads( @@failed_on_commit_retry_Qs, :commit, :io_failure )
      launch_storage_recovery_table_threads( :uploading )

      # queues, per storage_account_name, for failed notify, will continue to try resending
      @@failed_on_notify_retry_Qs = {}
      launch_storage_recovery_threads( @@failed_on_notify_retry_Qs, :notify, :notify_failed_blob_not_accessible )
      launch_storage_recovery_table_threads( :committed )

      # for failed to notify due to endpoint, will continue to try resending
      launch_endpoint_recovery_thread

      # queues, per storage_account_name, for failed to log to table, will continue to try resending
      @@failed_on_log_to_table_retry_Qs = {}
      launch_storage_recovery_threads( @@failed_on_log_to_table_retry_Qs, :log_to_table_update, :io_failure )

    end

    def self.close
      @@closing = true
    end

    def self.stopped?
      @@closing
    end

    def self.launch_endpoint_recovery_thread
      @@failed_on_notification_endpoint_retry_Q = Queue.new
      storage_recovery_thread( nil, @@failed_on_notification_endpoint_retry_Q, :notify, :io_failure )
    end

    def self.launch_storage_recovery_threads ( queues, method, failure_reason )
      @@configuration[:storage_account_name_key].each do |storage_account_name, storage_account_keys|
        queues[storage_account_name] = Queue.new
        # a threads, per storage  account name
        storage_recovery_thread( storage_account_name, queues[storage_account_name], method, failure_reason )
      end
    end

    def self.launch_storage_recovery_table_threads ( state )
      @@configuration[:storage_account_name_key].each do |storage_account_name, storage_account_keys|
        recovery_table_thread( storage_account_name, state)
      end
    end

    #return thread
    def self.recovery_table_thread( storage_account_name, state )
      Thread.new( storage_account_name, state ) do |storage_account_name, state|
        
        blob = Blob.new

        committed_tuples = [  ]
        uncommitted_tuples = [  ]
        upload_empty_tuples = [  ]
        token = nil
        finished = false
        filter = "#{:PartitionKey} eq '#{@@configuration[:azure_storage_blob_prefix]}-#{state}'"

        # should exit thread after fetching data from table, and submit recovery, the loop is only for case of failure
        until finished || stopped? do
          entities = blob.log_to_table_query( storage_account_name, filter, token )
          if entities
            token = entities.continuation_token

            if :committed == state
              entities.each do |entity|
                State.instance.inc_pending_notifications
                tuple = blob.table_entity_to_tuple( entity.properties )
                @@failed_on_notification_endpoint_retry_Q << tuple
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
              @@failed_on_log_to_table_retry_Qs[storage_account_name] << tuple
            end
            uncommitted_tuples.each do |tuple|
              State.instance.inc_pending_commits
              @@failed_on_commit_retry_Qs[storage_account_name] << tuple
            end
            upload_empty_tuples.each do |tuple|
              @@failed_on_log_to_table_retry_Qs[storage_account_name] << tuple
            end
            finished = true
          else
            Stud.stoppable_sleep(60, 1) { stopped? }
          end
        end
        @@logger.info { "exit table recovery thread, storage: #{storage_account_name}, state: #{state}, entities: #{entities ? entities.length : nil}" }
      end
    end

    def self.state_on? ( storage_account_name, blob, failure_reason )
      if blob
        if :io_failure == failure_reason
          @@endpoint_state_on ||= blob.test_notification_endpoint( @@configuration[:storage_account_name_key][0][0] )
        else
          Clients.instance.storage_account_state_on?( storage_account_name )
        end
      elsif storage_account_name
        Clients.instance.storage_account_state_on?( storage_account_name )
      else
        Clients.instance.storage_account_state_on?
      end
    end

    def self.storage_recovery_thread( storage_account_name, queue, method, failure_reason )
      # a threads, per storage  account name, that retries failed blob commits / notification / table updates
      Thread.new( storage_account_name, queue, method, failure_reason ) do |storage_account_name, queue, method, failure_reason|
        blob = Blob.new if :notify == method
        semaphore = Mutex.new
        action = {:method => method, :semaphore => semaphore, :counter => 0 }
        loop do
          tuple ||= queue.pop
          until state_on?( storage_account_name, blob, failure_reason ) do sleep( 1 ) end

          not_busy = nil
          semaphore.synchronize {
            not_busy = action[:counter] += 1 if 10 > action[:counter]
          }
          if not_busy
            Thread.new( action, tuple ) do |action, tuple|
              Blob.new.send( action[:method], tuple )
              action[:semaphore].synchronize {
                action[:counter] -= 1
              }
            end
            tuple = nil # release for GC
          else
            Stud.stoppable_sleep(60, 1) { 10 > action[:counter] }
            next
          end 
        end
      end
    end


    def initialize ( channel = nil, id = nil , no_queue = false )
      @uploaded_block_ids = [  ]
      @uploaded_block_numbers = [  ]
      @uploaded_bytesize = 0
      @uploaded_events_count = 0
      @max_tries = @@io_max_retries + 1
      @sub_state = :none

      if channel
        @id = id
        @intrumentation_key = channel.intrumentation_key
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
                to_commit = :commit if blob_full?
                upload( block_to_upload, to_commit)
                block_to_upload = nil # release reference to resource for GC
              else
                commit unless @uploaded_block_ids.empty?
              end

              if to_commit
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
        BLOB_MAX_BLOCKS < @uploaded_block_ids.length + 1 || @@blob_max_events < @uploaded_events_count + next_block.events_count || @@blob_max_bytesize < @uploaded_bytesize  + next_block.bytesize
      else
        BLOB_MAX_BLOCKS <= @uploaded_block_ids.length || @@blob_max_events <= @uploaded_events_count || @@blob_max_bytesize <= @uploaded_bytesize
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
      [ options[:start_time.to_s] || Time.now.utc, options[:action.to_s], options[:intrumentation_key.to_s], options[:table_id.to_s], 
        options[:storage_account_name.to_s], options[:container_name.to_s], options[:blob_name.to_s], 
        eval( options[:uploaded_block_ids.to_s] ), eval( options[:uploaded_block_numbers.to_s] ), 
        options[:uploaded_events_count.to_s] || 0, options[:uploaded_bytesize.to_s] || 0, options[:oldest_event_time.to_s] || Time.now.utc,
        options[:event_format_ext.to_s], options[:blob_max_delay.to_s] || 0,
        options[:log_state.to_s].to_sym, (options[:sub_state.to_s] || :none).to_sym
      ]
    end

    def state_to_tuple
      [ @start_time || Time.now.utc, @action, @intrumentation_key, @table_id, 
        @storage_account_name, @container_name, @blob_name, 
        @uploaded_block_ids, @uploaded_block_numbers, 
        @uploaded_events_count, @uploaded_bytesize, @oldest_event_time,
        @event_format_ext, @blob_max_delay,
        @log_state, @sub_state
      ]
    end

    def tuple_to_state ( tuple )
      ( @start_time, @action, @intrumentation_key, @table_id, 
        @storage_account_name, @container_name, @blob_name, 
        @uploaded_block_ids, @uploaded_block_numbers, 
        @uploaded_events_count, @uploaded_bytesize, @oldest_event_time,
        @event_format_ext, @blob_max_delay,
        @log_state, @sub_state) = tuple
    end

    def state_to_table_entity
      { :start_time => @start_time, :intrumentation_key => @intrumentation_key, :table_id => @table_id, 
        :storage_account_name => @storage_account_name, :container_name => @container_name, :blob_name => @blob_name, 
        :uploaded_block_ids => @uploaded_block_ids.to_s, :uploaded_block_numbers => @uploaded_block_numbers.to_s, 
        :uploaded_events_count => @uploaded_events_count, :uploaded_bytesize => @uploaded_bytesize, :oldest_event_time => @oldest_event_time,
        :log_state => @log_state, :sub_state => @sub_state
      }
    end


    def test_storage_recover
      proc do |reason, e| @recovery = :ok if :container_exist == reason || :create_container == reason end
    end


    def test_storage ( storage_account_name )
      @storage_account_name = storage_account_name
      @action = :test_storage
      @max_tries = 1
      @force_client = true # to enable get a client even if all storage_accounts marked dead
      @recoverable = [ :invalid_storage_key ]
      storage_io_block( test_storage_recover ) {
        if @recovery.nil? || :invalid_storage_key == @recovery
          container_name = "logstash-test-container"
          @info = "#{@action} #{@storage_account_name}"
          @client.blobClient.create_container( container_name ) unless @@configuration[:disable_blob_upload]
        end
      }
    end

    def test_notification_endpoint_recover
      proc do |reason, e| @recovery = :ok if :invalid_intrumentation_key == reason || :invalid_table_id == reason end
    end

    def test_notification_endpoint( storage_account_name )
      @storage_account_name = storage_account_name
      @action = :test_notification_endpoint
      @max_tries = 1
      @force_client = true # to enable get a client even if all storage_accounts marked dead
      @recoverable = [  ]
      success = storage_io_block( test_notification_endpoint_recover ) {
        if @recovery.nil?
          @container_name = "logstash-test-container"
          @blob_name = "logstash-test-blob"
          @table_id = GUID_NULL
          @intrumentation_key = GUID_NULL
          @info = "#{@action}"
          set_blob_sas_url
          payload = create_payload
          post_notification( @client.notifyClient, payload )
        end
      }
      sleep( 30 ) unless success
      success
    end


    def notify_recover
      proc do |reason, e|
        if :notify_failed_blob_not_accessible == reason
          @sub_state = reason
          @@failed_on_notify_retry_Qs[@storage_account_name] << state_to_tuple
        elsif :invalid_intrumentation_key == reason || :invalid_table_id == reason
          @sub_state = reason
          Channels.instance.channel( @intrumentation_key, @table_id ).failed_on_notify_retry_Q << state_to_tuple

        else
          @@endpoint_state_on = false
          if :notify_failed_blob_not_accessible == @sub_state
            @@failed_on_notify_retry_Qs[@storage_account_name] << state_to_tuple
          elsif :invalid_intrumentation_key == @sub_state || :invalid_table_id == @sub_state
            Channels.instance.channel( @intrumentation_key, @table_id ).failed_on_notify_retry_Q << state_to_tuple
          else
            @@failed_on_notification_endpoint_retry_Q << state_to_tuple
          end
        end
      end
    end

    def notify ( tuple = nil )
      tuple_to_state( tuple ) if tuple
      @action = :notify
      @force_client = true # to enable get a client even if all storage_accounts marked dead
      @recoverable = [ :notify_failed_blob_not_accessible, :io_failure, :service_unavailable ]
      success = storage_io_block( notify_recover ) {
        set_blob_sas_url
        payload = create_payload
        @@logger.debug { "notification payload: #{payload}" }
        @info = "#{@action.to_s} #{@storage_account_name}/#{@container_name}/#{@blob_name}, events: #{@uploaded_events_count}, size: #{@uploaded_bytesize}, blocks: #{@uploaded_block_numbers}, delay: #{Time.now.utc - @oldest_event_time}, blob_sas_url: #{@blob_sas_url}"

        # assume that exceptions can be raised due to this method:
        post_notification( @client.notifyClient, payload ) unless @@configuration[:disable_notification]
        @log_state = :notified
      }
      log_to_table_update if success
    end

    CREATE_EXIST_ERRORS = { :container => [ :create_container, :container_exist ], :table => [ :create_table, :table_exist ] }
    def create_exist_recovery( type, name = nil )
      prev_info = @info
      if CREATE_EXIST_ERRORS[type][0] == @recovery
        name ||= ( :table == type ? @@state_table_name : @container_name )
        @info = "create #{type} #{@storage_account_name}/#{name}"

        # assume that exceptions can be raised due to this method:
        yield name
        @@logger.info { "Successed to #{@info}" }
        @info = prev_info
      elsif CREATE_EXIST_ERRORS[type][1] == @recovery
        @@logger.info { "Successed (already exist) to #{@info}" }
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
    def log_to_table_insert
      @action = :log_to_table_insert
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable, :table_exist, :create_table, :table_busy, :entity_exist ]
      @info  = "#{@action} #{@log_state} #{@storage_account_name}/#{@container_name}/#{@blob_name}"
      success =  storage_io_block( :uploading == @log_state ? proc do |reason, e| end : log_to_table_update_recover ) {
        create_table_exist_recovery
        if :entity_exist == @recovery
          raise NotRecoverableError if :uploading == @log_state
        else
          entity_values = state_to_table_entity
          entity_values[:PartitionKey] = "#{@@configuration[:azure_storage_blob_prefix]}-#{@log_state}"
          entity_values[:RowKey] = @blob_name
          @client.tableClient.insert_entity( @@state_table_name, entity_values )
        end
      }
    end

    def log_to_table_update_recover
      proc do |reason, e| @@failed_on_log_to_table_retry_Qs[@storage_account_name] << state_to_tuple end
    end

    def log_to_table_update ( tuple = nil )
      tuple_to_state( tuple ) if tuple
      if :uploading == @log_state
        log_to_table_delete
      elsif :committed == @log_state
        if log_to_table_insert && log_to_table_delete( nil, :uploading )
          State.instance.dec_pending_commits
          State.instance.inc_pending_notifications
          @@failed_on_notification_endpoint_retry_Q << state_to_tuple
        end
      elsif :notified == @log_state
        if (!@@save_notified_blobs_records || log_to_table_insert) && log_to_table_delete( nil, :committed ) 
          State.instance.dec_pending_notifications
        end
      end
    end


    # retturn tru on success
    def log_to_table_delete ( tuple = nil, state = nil )
      tuple_to_state( tuple ) if tuple
      state ||= @log_state
      @action = :log_to_table_delete
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable, :table_exist, :create_table, :table_busy, :create_resource ]
      @info  = "#{@action} #{state} #{@storage_account_name}/#{@container_name}/#{@blob_name}"

      success =  storage_io_block( log_to_table_update_recover ) {
        create_table_exist_recovery
        if :create_resource == @recovery
          @@logger.info { "Note: delete entity failed, already deleted, #{@info}, state: #{state}, log_state: #{@log_state}" }
        else
          @client.tableClient.delete_entity( @@state_table_name, "#{@@configuration[:azure_storage_blob_prefix]}-#{state}", @blob_name )
        end
      }
    end

    # return entities
    def log_to_table_query ( storage_account_name, filter , token )
      @storage_account_name = storage_account_name

      @action = :log_to_table_query
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable, :table_exist, :create_table, :table_busy ]
      @info  = "#{@action} #{@storage_account_name}/#{@@state_table_name}"

      entities = nil
      success =  storage_io_block( proc do |reason, e| end ) {
        create_table_exist_recovery
        options = { :filter => filter }
        options[:continuation_token] = token if token
        entities = @client.tableClient.query_entities( @@state_table_name, options )
      }
      entities
    end

    def commit_recover
      proc do |reason, e| @@failed_on_commit_retry_Qs[@storage_account_name] << state_to_tuple end
    end

    def commit ( tuple = nil )
      tuple_to_state( tuple ) if tuple

      unless @uploaded_block_ids.empty?
        @action = :commit
        @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable ]
        success =  storage_io_block( commit_recover ) {
          @info = "#{@action.to_s} #{@storage_account_name}/#{@container_name}/#{@blob_name}, events: #{@uploaded_events_count}, size: #{@uploaded_bytesize}, blocks: #{@uploaded_block_numbers}, delay: #{Time.now.utc - @oldest_event_time}"
          # assume that exceptions can be raised due to this method:
          @client.blobClient.commit_blob_blocks( @container_name, @blob_name, @uploaded_block_ids ) unless @@configuration[:disable_blob_upload]
          @log_state = :committed
        }
        # next stage
        log_to_table_update if success
      end
    end


    def upload_recover
      proc do |reason, e|
        unless @uploaded_block_ids.empty?
          info1 = "#{:commit} #{@storage_account_name}/#{@container_name}/#{@blob_name}, events: #{@uploaded_events_count}, size: #{@uploaded_bytesize}, blocks: #{@uploaded_block_numbers}, delay: #{Time.now.utc - @oldest_event_time}"
          @@logger.error { "Pospone to #{info1} (; retry later, error: #{e.inspect}" }
          @@failed_on_commit_retry_Qs[@storage_account_name] << state_to_tuple
          @uploaded_block_ids = [  ]
        end
        unless :io_all_dead == reason
          @recovery = :invalid_storage_account
        else 
          Channels.instance.channel( @intrumentation_key, @table_id ).failed_on_upload_retry_Q << @block_to_upload
          @block_to_upload = nil
        end
      end
    end

    def upload ( block, to_commit = nil )
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
            @@failed_on_log_to_table_retry_Qs[@storage_account_name] << state_to_tuple
          end
          set_conatainer_and_blob_names
          @storage_account_name = Clients.instance.get_random_active_storage( exclude_storage_account_names )
          unless @storage_account_name
            upload_recover.call( :io_all_dead, nil )
            return false
          end
          raise UploadRetryError unless log_to_table_insert
        end

        @action = :upload
        @block_info = "blocks: #{@block_to_upload.block_numbers}, events: #{@block_to_upload.events_count}, size: #{@block_to_upload.bytes.length}"
        @info = "#{@action} #{@storage_account_name}/#{@container_name}/#{@blob_name}, #{@block_info}, commitId: [\"#{100001 + @uploaded_block_ids.length}\"]"
        @recoverable = [ :invalid_storage_key, :invalid_storage_account, :io_failure, :service_unavailable, :container_exist, :create_container ]

        success = storage_io_block( upload_recover ) {
          create_container_exist_recovery
          block_id = "#{100001 + @uploaded_block_ids.length}"

          # assume that exceptions can be raised due to this method:
          @client.blobClient.put_blob_block( @container_name, @blob_name, block_id, @block_to_upload.bytes ) unless @@configuration[:disable_blob_upload]

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
          Telemetry.instance.track_event("uploading", {:properties => state_to_table_entity})
        }

        raise UploadRetryError if :invalid_storage_account == @recovery
        commit if success && to_commit
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
      success =  storage_io_block( proc do |reason, e| end ) {
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


    def storage_io_block( recover_later_proc, valid_recovery = nil )
      @recovery = nil
      @try_count = 1

      begin
        @client ||= Client.new( @storage_account_name, @force_client )
        yield
        disabled = :notify == @action ? @@configuration[:disable_notification] : @@configuration[:disable_blob_upload]
        @@logger.info { "Successed to #{disabled ? 'DISABLED ' : ''}#{@info}" }
        true

      rescue TypeError
        raise

      rescue StandardError => e
        @recovery = nil
        retry if recover_retry?( e, recover_later_proc )
        false

      ensure
        @client = @client.dispose if @client
      end
    end


    def recover_retry? ( e, recover_later_proc )
      # http error, probably server error
      if e.is_a?( Azure::Core::Http::HTTPError )

        if 404 == e.status_code && "ContainerNotFound" == e.type
          @recovery = :create_container

        elsif 404 == e.status_code && "TableNotFound" == e.type
          @recovery = :create_table

        elsif 404 == e.status_code && "BlobNotFound" == e.type
          @recovery = :create_blob
          
        elsif 404 == e.status_code && "ResourceNotFound" == e.type
          @recovery = :create_resource

        elsif 409 == e.status_code && "ContainerAlreadyExists" == e.type
          @recovery = :container_exist

        elsif 409 == e.status_code && "BlobAlreadyExists" == e.type
          @recovery = :blob_exist

        elsif 409 == e.status_code && "TableAlreadyExists" == e.type
          @recovery = :table_exist

        elsif 409 == e.status_code && "TableBeingDeleted" == e.type
          @recovery = :table_busy

        elsif 409 == e.status_code && "EntityAlreadyExists" == e.type
          @recovery = :entity_exist

        elsif 403 == e.status_code && "AuthenticationFailed" == e.type
          @recovery = :invalid_storage_key

        elsif 403 == e.status_code  && "Unknown" == e.type && e.description.include?("Blob does not exist or not accessible.")
          @recovery = :notify_failed_blob_not_accessible

        elsif 400 == e.status_code  && "Unknown" == e.type && e.description.include?("Invalid instrumentation key")
          @recovery = :invalid_intrumentation_key

        elsif 500 == e.status_code  && "Unknown" == e.type && e.description.include?("Processing error")
          @recovery = :notification_process_down

        elsif 503 == e.status_code
          @recovery = :service_unavailable
        elsif 404 == e.status_code
          @recovery = :create_resource
        elsif 403 == e.status_code
          # todo, came from updating the log_table, how to hnadle this
          @recovery = :access_denied
        else
          puts "\n>>>> HTTP error - #{e.inspect} <<<<\n"
          @recovery = :http_unknown
          raise e if  @@configuration[:stop_on_unknown_io_errors]
        end

      # communication error
      elsif e.is_a?( Faraday::ClientError )
        @recovery = :io_failure

      # communication error
      elsif e.is_a?( IOError )
        @recovery = :io_failure

      # all storage accounts are dead, couldn't get client (internal exception)
      elsif e.is_a?( StorageAccountsOffError )
        @recovery = :io_all_dead

      # all storage accounts are dead, couldn't get client (internal exception)
      elsif e.is_a?( NotRecoverableError )
        @recovery = :not_recoverable

      elsif e.is_a?( NameError ) && e.message.include?( "uninitialized constant Azure::Core::Auth::Signer::OpenSSL" )
        sleep( 1 )
        @recovery = :io_failure

      else
        # UNKNOWN error - #<NameError: uninitialized constant Azure::Core::Auth::Signer::OpenSSL>
        puts "\n>>>> UNKNOWN error - #{e.inspect} <<<<\n"
        raise e

      end

      reason = @recovery
      if @recovery && @recoverable.include?( @recovery )
        case @recovery
        when :container_exist, :table_exist, :entity_exist, :create_container, :create_table
          # ignore log error
          # @@logger.error { "Failed to #{@info} ;( recovery: continue, error: #{e.inspect}" }

        when :invalid_storage_key, :notify_failed_blob_not_accessible
          if @client.switch_storage_account_key!
            @@logger.error { "Failed to #{@info} ;( recovery: switched to secondary storage key, error: #{e.inspect}" }
          else
            @client = @client.dispose( :auth_to_storage_failed ) if @client && :invalid_storage_key == @recovery 
            @recovery = nil
          end

        when :table_busy
          @client = @client.dispose if @client
          sleep( @@io_retry_delay )
          @@logger.error { "Failed to #{@info} ;( recovery: retry, error: #{e.inspect}" }

        when :io_failure, :service_unavailable, :notification_process_down, :invalid_intrumentation_key, :invalid_table_id
          if @try_count < @max_tries
            @client = @client.dispose if @client
            sleep( @@io_retry_delay )
            @@logger.error { "Failed to #{@info} ;( recovery: retry, try #{@try_count} / #{@max_tries}, error: #{e.inspect}" }
            @try_count += 1
          else
            if :invalid_intrumentation_key == @recovery
              Channels.instance.mark_invalid_intrumentation_key( @intrumentation_key )
            elsif :invalid_table_id == @recovery
              Channels.instance.mark_invalid_table_id( @table_id )
            elsif :io_failure == @recovery || ( :service_unavailable == @recovery && :notify != @action )
              @client = @client.dispose( :io_to_storage_failed ) if @client
            end
            @recovery = nil
          end
        end
      else
        @recovery = nil
      end

      if @recovery
        true
      else
        recover_later_proc.call( reason, e )
        @@logger.error { "Failed to #{@info} ; retry later, error: #{e.inspect}" } unless :ok == @recovery
        :ok == @recovery
      end

      # Blob service error codes - msdn.microsoft.com/en-us/library/azure/dd179439.aspx
      # ConnectionFailed - problem with connection
      # ParsingError - problem with request/response payload
      # ResourceNotFound, SSLError, TimeoutError
    end

    def set_conatainer_and_blob_names
      time_utc = Time.now.utc
      id = @id.to_s.rjust(4, "0")
      strtime = time_utc.strftime( "%F" )
      @container_name = "#{@@configuration[:azure_storage_container_prefix]}-#{strtime}"

      strtime = time_utc.strftime( "%F-%H-%M-%S-%L" )
      @blob_name = "#{@@configuration[:azure_storage_blob_prefix]}_ikey-#{@intrumentation_key}_table-#{@table_id}_id-#{id}_#{strtime}.#{@event_format_ext}"
    end


    def create_payload
      notification_hash = {
        :data => {
          :baseType => DATA_BASE_TYPE,
          :baseData => {
            :ver           => BASE_DATA_REQUIRED_VERSION,
            :blobSasUri    => @blob_sas_url.to_s,
            :sourceName    => @table_id,
            :sourceVersion => @@configuration[:notification_version].to_s
          }
        }, 
        :ver  => @@configuration[:notification_version], 
        :name => REQUEST_NAME,
        :time => Time.now.utc.iso8601,
        :iKey => @intrumentation_key
      }
      notification_hash.to_json
    end 


    def post_notification ( http_client, body )
      request = Azure::Core::Http::HttpRequest.new( :post, @@configuration[:notification_endpoint], { :body => body, :client => http_client } )
      request.headers['Content-Type'] = 'application/json; charset=utf-8'
      request.headers['Accept'] = 'application/json'
      @@logger.debug { "send notification : \n    endpoint: #{@@configuration[:notification_endpoint]}\n    body : #{body}" }
      response = request.call
    end 


    def set_blob_sas_url
      blob_url ="https://#{@storage_account_name}.blob.core.windows.net/#{@container_name}/#{@blob_name}"
      options_and_constrains = {:permissions => "r", :resource => "b", :expiry => ( Time.now.utc + @@configuration[:blob_access_expiry_time] ).iso8601 }
      @blob_sas_url = @client.storage_auth_sas.signed_uri( URI( blob_url ), options_and_constrains )
    end

  end
end
