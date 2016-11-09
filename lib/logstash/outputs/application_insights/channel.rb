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
  class Channel

    attr_reader :instrumentation_key
    attr_reader :table_id
    attr_reader :blob_max_delay
    attr_reader :blob_extension
    attr_reader :event_format

    public

    def initialize ( instrumentation_key, table_id )
      @closing = false
      configuration = Config.current
      
      @file_pipe = !configuration[:disable_compression]
      @gzip_file = !configuration[:disable_compression]
      @blob_max_bytesize = configuration[:blob_max_bytesize]
      @blob_max_events = configuration[:blob_max_events]

      @logger = configuration[:logger]

      @logger.debug { "Create a new channel, instrumentation_key / table_id : #{instrumentation_key} / #{table_id}" }
      @instrumentation_key = instrumentation_key
      @table_id = table_id
      set_table_properties( configuration )
      @semaphore = Mutex.new
      @workers_channel = {  }

      @failed_on_notify_retry_Q = Queue.new
      launch_notify_recovery_thread

      @blob_extension = ".#{@event_format}"
      if file_pipe?
        @blob_extension = "_#{@event_format}.gz" if gzip_file?
        @add_pipe_threshold = 0
        @file_prefix = configuration[:local_file_prefix]
        @file = nil
        @failed_on_file_upload_retry_Q = Queue.new
        launch_file_upload_recovery_thread
      else
        @add_pipe_threshold = CHANNEL_THRESHOLD_TO_ADD_UPLOAD_PIPE
        @failed_on_block_upload_retry_Q = Queue.new
        launch_block_upload_recovery_thread
      end

      @active_upload_pipes = [ Upload_pipe.new( self, 1 ) ]
    end


    def gzip_file?
      @gzip_file
    end

    def file_pipe?
      @file_pipe
    end

    def close
      @closing = true
      @active_upload_pipes.each do |upload_pipe|
        upload_pipe.close
      end
    end

    def stopped?
      @closing
    end

    # received data is an hash of the event (does not include metadata)
    def << ( data )
      if @serialized_event_field && data[@serialized_event_field]
        serialized_event = serialize_serialized_event_field( data[@serialized_event_field] )
      else
        serialized_event = ( EXT_EVENT_FORMAT_CSV == @event_format ? serialize_to_csv( data ) : serialize_to_json( data ) )
      end

      if serialized_event
        sub_channel = @workers_channel[Thread.current] || @semaphore.synchronize { @workers_channel[Thread.current] = Sub_channel.new( @event_separator ) }
        sub_channel << serialized_event
      else
        @logger.warn { "event not uploaded, no relevant data in event. table_id: #{table_id}, event: #{data}" }
      end
    end


    def flush
      if file_pipe?
        gz_collect_and_compress_blocks_to_file
        if file_expired_or_full?
          enqueue_to_pipe( [ @file ] )
          @file = nil
        end
      else
        list = collect_blocks
        enqueue_to_pipe( list )
      end
    end


    def recover_later_notification( tuple )
      @failed_on_notify_retry_Q << tuple
    end


    def recover_later_block_upload( block_to_upload )
      @failed_on_block_upload_retry_Q << block_to_upload
    end

    def recover_later_file_upload( file_to_upload )
      # start the file from the begining
      file_to_upload.close_read
      @failed_on_file_upload_retry_Q << file_to_upload
    end

    private

    def local_file_name
      time_utc = Time.now.utc
      strtime = Time.now.utc.strftime( "%F-%H-%M-%S-%L" )
      "#{@file_prefix}_ikey-#{@instrumentation_key}_table-#{@table_id}_#{strtime}#{@blob_extension}"
    end


    def local_file
      @file ||= Local_file.new( local_file_name, gzip_file? )
    end


    def file_expired_or_full?
      @file && ( @file.oldest_event_time + @blob_max_delay <= Time.now.utc  ||  @file.bytesize >= @blob_max_bytesize  ||  @file.events_count >= @blob_max_events )
    end


    def gz_collect_and_compress_blocks_to_file
      workers_channel = @semaphore.synchronize { @workers_channel.dup }
      full_block_list = [  ]

      workers_channel.each_value do |worker_channel|
        full_block_list.concat( worker_channel.get_block_list! )
      end

      full_block_list.each do |block|
        block.partial_seal
        local_file << block
      end
    end


    def launch_file_upload_recovery_thread
      #recovery thread
      Thread.new do
        loop do
          file_to_upload = @failed_on_file_upload_retry_Q.pop
          until Clients.instance.storage_account_state_on? do
            Stud.stoppable_sleep( 60 ) { stopped? }
          end
          if file_to_upload
            enqueue_to_pipe( [ file_to_upload ] )
          end
        end
      end
    end


    def collect_blocks
      workers_channel = @semaphore.synchronize { @workers_channel.dup }
      full_block_list = [  ]
      prev_last_block = nil

      workers_channel.each_value do |worker_channel|
        block_list = worker_channel.get_block_list!
        unless block_list.empty?
          last_block = block_list.pop
          full_block_list.concat( block_list )
          if prev_last_block
            unless prev_last_block.concat( last_block )
              full_block_list << prev_last_block
              prev_last_block = last_block
            end
          else
            prev_last_block = last_block
          end
        end
      end
      full_block_list << prev_last_block if prev_last_block
      full_block_list
    end


    def enqueue_to_pipe ( list )
      list.each do |block_or_file|
        block_or_file.seal
        find_upload_pipe << block_or_file
      end
    end


    def launch_block_upload_recovery_thread
      #recovery thread
      Thread.new do
        loop do
          block_to_upload = @failed_on_block_upload_retry_Q.pop
          until Clients.instance.storage_account_state_on? do
            Stud.stoppable_sleep( 60 ) { stopped? }
          end
          if block_to_upload
            enqueue_to_pipe( [ block_to_upload ] )
          end
        end
      end
    end


    # thread that failed to notify due to Application Insights error, such as wrong key or wrong schema
    def launch_notify_recovery_thread
      #recovery thread
      Thread.new do
        loop do
          tuple = @failed_on_notify_retry_Q.pop
          begin
            Stud.stoppable_sleep( 60 ) { stopped? }
          end until Clients.instance.storage_account_state_on? || stopped?

          if  stopped?
            @state ||= State.instance
            @state.dec_pending_notifications
            @shutdown ||= Shutdown.instance
            @shutdown.display_msg("!!! notification won't recover in this session due to shutdown")
          else
            success = Notification.new( tuple ).notify
            while success && @failed_on_notify_retry_Q.length > 0
              tuple = @failed_on_notify_retry_Q.pop
              success = Notification.new( tuple ).notify
            end
          end
          tuple = nil # release for GC
        end
      end
    end


    def serialize_serialized_event_field ( data )
      serialized_data = nil
      if data.is_a?( String )
        serialized_data = data
      elsif EXT_EVENT_FORMAT_CSV == @event_format
        if data.is_a?( Array )
          serialized_data = data.to_csv( :col_sep => @csv_separator )
        elsif data.is_a?( Hash )
          serialized_data = serialize_to_csv( data )
        end
      elsif EXT_EVENT_FORMAT_JSON == @event_format
        if data.is_a?( Hash )
          serialized_data = serialize_to_json( data )
        elsif data.is_a?( Array ) && !@table_columns.nil?
          serialized_data = serialize_to_json( Hash[@table_columns.map {|column| column[:name]}.zip( data )] )
        end
      end
      serialized_data
    end


    def serialize_to_json ( data )
      return data.to_json unless !@table_columns.nil?

      data = Utils.downcase_hash_keys( data ) if @case_insensitive_columns

      json_hash = {  }
      @table_columns.each do |column|
        value = data[column[:field_name]] || column[:default]
        json_hash[column[:name]] = value if value
      end
      return nil if json_hash.empty?
      json_hash.to_json
    end


    def serialize_to_csv ( data )
      return nil unless !@table_columns.nil?

      data = Utils.downcase_hash_keys( data ) if @case_insensitive_columns

      csv_array = [  ]
      @table_columns.each do |column|
        value = data[column[:field_name]] || column[:default] || @csv_default_value
        type = (column[:type] || value.class.name).downcase.to_sym
        csv_array << ( [:hash, :array, :json, :dynamic, :object].include?( type ) ? value.to_json : value )
      end
      return nil if csv_array.empty?
      csv_array.to_csv( :col_sep => @csv_separator )
    end


    def find_upload_pipe
      min_upload_pipe = @active_upload_pipes[0]
      @active_upload_pipes.each do |upload_pipe|
        return upload_pipe unless min_upload_pipe.busy?
        min_upload_pipe = upload_pipe if upload_pipe.queue_size < min_upload_pipe.queue_size
      end
      @active_upload_pipes << ( min_upload_pipe = Upload_pipe.new( self, @active_upload_pipes.length + 1 ) ) if min_upload_pipe.busy? && min_upload_pipe.queue_size >= @add_pipe_threshold && @active_upload_pipes.length < MAX_CHANNEL_UPLOAD_PIPES
      min_upload_pipe
    end


    def set_table_properties ( configuration )
      table_properties = configuration[:tables][@table_id]

      if table_properties
        @blob_max_delay = table_properties[:blob_max_delay]
        @event_separator = table_properties[:event_separator]
        @serialized_event_field = table_properties[:serialized_event_field]
        @table_columns = table_properties[:table_columns]
        @event_format = table_properties[:blob_serialization]
        @case_insensitive_columns = table_properties[:case_insensitive_columns]
        @csv_default_value = table_properties[:csv_default_value]
        @csv_separator = table_properties[:csv_separator]
      end

      @blob_max_delay ||= configuration[:blob_max_delay]
      @event_separator ||= configuration[:event_separator]
      @serialized_event_field ||= configuration[:serialized_event_field]
      @table_columns ||= configuration[:table_columns]
      @event_format ||= configuration[:blob_serialization]
      @case_insensitive_columns ||= configuration[:case_insensitive_columns]
      @csv_default_value ||= configuration[:csv_default_value]
      @csv_separator ||= configuration[:csv_separator]

      # add field_name to each column, it is required to differentiate between the filed name and the column name
      unless @table_columns.nil?
        @table_columns = @table_columns.map do |column|
          new_column = column.dup
          new_column[:field_name] = ( @case_insensitive_columns ? new_column[:name].downcase : new_column[:name] )
          new_column
        end
      end

    end

  end
end
