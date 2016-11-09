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
  class Upload_pipe < Blob


    def initialize ( channel = nil, id = nil, tuple = nil )

      # super first parameter must be nil. blob first parameter is channel, otherwise it will pass storage_account_name as channel
      super( tuple )
      @channel = channel
      if @channel
        @id = id
        @instrumentation_key = @channel.instrumentation_key
        @table_id = @channel.table_id
        @blob_max_delay = @channel.blob_max_delay
        @blob_extension = @channel.blob_extension
        @event_format = @channel.event_format
        @file_pipe = @channel.file_pipe?

        @io_queue = Queue.new

        # create a thread that handles the IO of the blob
        if @file_pipe
          launch_file_pipe_thread
        else
          launch_block_pipe_thread
        end
      end

    end


    def launch_block_pipe_thread
      Thread.new do
        timer = Timer.new
        next_block = nil
        loop do
          block_to_upload = nil # release reference to resource for GC
          block_to_upload = next_block || @io_queue.pop
          next_block = nil

          if :trigger == timer.state
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
            timer.set( block_to_upload.oldest_event_time + @blob_max_delay, nil ) {|object| @io_queue << :wakeup if 0 == @io_queue.length } if blob_empty?
            upload( block_to_upload )
            block_to_upload = nil # release reference to resource for GC
            to_commit = :commit if blob_full?
          end

          if to_commit
            commit unless @uploaded_block_ids.empty?
            to_commit = nil
            @uploaded_block_ids = [  ]
            timer.cancel
            break if :close == block_to_upload
          end
        end
      end
    end


    def launch_file_pipe_thread
      Thread.new do
        loop do
          file_to_upload = @io_queue.pop

          break if :close == file_to_upload

          @file_size = nil
          while block = file_to_upload.get_next_block
            @file_size ||= file_to_upload.file_size
            unless upload( block )
              @channel.recover_later_file_upload( file_to_upload )
              file_to_upload = nil
              @uploaded_block_ids = [  ]
              break
            end
          end
          file_to_upload.dispose if file_to_upload
          file_to_upload = nil

          commit unless @uploaded_block_ids.empty?
          @uploaded_block_ids = [  ]
        end
      end
    end


    # close blob. It will finish whatever was already on the queue, and if necessary commit
    # called on shutdown
    def close
      @io_queue << :close
    end


    def queue_size
      @io_queue.length
    end

    def busy?
      0 < @io_queue.length  ||  0 == @io_queue.num_waiting
    end

    def << ( block )
      @io_queue << block
    end


    def commit
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
          @storage_recovery.recover_later( context_to_tuple, :commit, @storage_account_name )
        end
      end
    end

    private

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


    def upload_retry_later
      
      unless @uploaded_block_ids.empty?
        if @file_pipe
          # remove "loading" record from state table of all previous blocks uploaded , we will try the whole file on an alternative storage
          @storage_recovery.recover_later( context_to_tuple, :state_table_update, @storage_account_name )
          @block_to_upload = nil
          return
        else
          info1 = "#{:commit} #{@storage_account_name}/#{@container_name}/#{@blob_name}, events: #{@uploaded_events_count}, size: #{@uploaded_bytesize}, blocks: #{@uploaded_block_numbers}, delay: #{Time.now.utc - @oldest_event_time}"
          @logger.error { "Pospone to #{info1} (; retry later, error: #{@last_io_exception.inspect}" }
          @storage_recovery.recover_later( context_to_tuple, :commit, @storage_account_name )
          @uploaded_block_ids = [  ]
        end
      end
      if :io_all_dead == @recovery
        @channel.recover_later_block_upload( @block_to_upload ) unless @file_pipe
        @block_to_upload = nil
      else 
        raise UploadRetryError
      end
    end

    # returns success / failure
    def upload ( block )
      @storage_account_name = nil if @uploaded_block_ids.empty?
      @block_to_upload = block
      block = nil # remove reference for GC
      exclude_storage_account_names = [  ]
      success = false
      begin
        if @uploaded_block_ids.empty?
          @log_state = :uploading
          @uploaded_block_numbers = [  ]
          @uploaded_bytesize = 0
          @uploaded_events_count = 0
          @oldest_event_time = nil

          # remove "loading" record from state table of first block upload that failed, we will try on alternative storage
          if @storage_account_name
            exclude_storage_account_names << @storage_account_name
            @storage_recovery.recover_later( context_to_tuple, :state_table_update, @storage_account_name )
          end
          set_conatainer_and_blob_names
          @storage_account_name = Clients.instance.get_random_active_storage( exclude_storage_account_names )
          unless @storage_account_name
            @recovery = :io_all_dead
            upload_retry_later
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
      success
    end


    def set_conatainer_and_blob_names
      time_utc = Time.now.utc
      id = @id.to_s.rjust(4, "0")
      strtime = time_utc.strftime( "%F" )
      @container_name = "#{AZURE_STORAGE_CONTAINER_LOGSTASH_PREFIX}#{@configuration[:azure_storage_container_prefix]}-#{strtime}"

      strtime = time_utc.strftime( "%F-%H-%M-%S-%L" )
      @blob_name = "#{AZURE_STORAGE_BLOB_LOGSTASH_PREFIX}#{@configuration[:azure_storage_blob_prefix]}/ikey-#{@instrumentation_key}/table-#{@table_id}/#{strtime}_#{id}#{@blob_extension}"
    end

  end
end
