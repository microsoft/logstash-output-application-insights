# encoding: utf-8

class LogStash::Outputs::Msai
  class Channel

    attr_reader :intrumentation_key
    attr_reader :table_id
    attr_reader :failed_on_upload_retry_Q
    attr_reader :event_format_ext
    attr_reader :blob_max_delay

    public

    def initialize ( intrumentation_key, table_id )
      configuration = Config.current

      @logger = configuration[:logger]

      @logger.debug { "Create a new channel, intrumentation_key / table_id : #{intrumentation_key} / #{table_id}" }
      @intrumentation_key = intrumentation_key
      @table_id = table_id
      set_event_format_ext( configuration )
      set_table_properties( configuration )
      @semaphore = Mutex.new
      @failed_on_upload_retry_Q = Queue.new
      @sub_channels = {  }
      @active_blobs = [ Blob.new( self, 1 ) ]

      launch_recovery_thread
    end

    def close_active_blobs
      @active_blobs.each do |blob|
        blob.close
      end
    end


    def << ( event )
      serialized_event = ( @csv_map ? serialize_to_csv( event ) :( @data_field ? serialize_to_data_field( event ) : serialize_to_json( event ) ) )
      if serialized_event
        sub_channel = @sub_channels[Thread.current] || @semaphore.synchronize { @sub_channels[Thread.current] = Sub_channel.new( @event_separator ) }
        sub_channel << serialized_event 
      end
    end


    def collect_blocks
      sub_channels = @semaphore.synchronize { @sub_channels.dup }
      full_block_list = [  ]
      prev_last_block = nil

      sub_channels.each_value do |sub_channel|
        block_list = sub_channel.get_block_list!
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


    def enqueue_blocks ( block_list )
      block_list.each do |block|
        block.seal
        find_blob << block
      end
    end


    private

    def launch_recovery_thread
      #recovery thread
      Thread.new do
        next_block = nil
        loop do
          block_to_upload = next_block || @failed_on_upload_retry_Q.pop
          next_block = nil
          until Clients.instance.storage_account_state_on? do
            Stud.stoppable_sleep( 60 ) { stopped? }
          end
          if block_to_upload
            find_blob << block_to_upload
          end
        end
      end
    end


    def serialize_to_data_field ( event )
      event_data = event[@data_field]
      @logger.warn { "event not uploaded, because field #{@data_field} was empty. table_id: #{table_id}, event: #{event}" } unless event_data
      event_data
    end

    def serialize_to_json ( event )
      event.to_json
    end

    def serialize_to_csv ( event )
        csv_array = []
        @csv_map.each do |column|
          value = event[column[:name]] || column[:default] || @csv_default_value
          case (column[:type] || value.class.name).downcase.to_sym
          when :string
            csv_array << value
          when :hash, :array, :json, :dynamic, :object
            csv_array << value.to_json
          when :integer
            csv_array << value
          when :number, :float
            csv_array << value
          when :datetime
            csv_array << value
          when :boolean
            csv_array << value
          else
            csv_array << value
          end
        end
        csv_array.to_csv( :col_sep => @csv_separator )
    end


    def find_blob
      min_blob = @active_blobs[0]
      @active_blobs.each do |blob|
        return blob if 0 == blob.queue_size
        min_blob = blob if blob.queue_size < min_blob.queue_size
      end
      @active_blobs << ( min_blob = Blob.new( self, @active_blobs.length + 1 ) ) if min_blob.queue_size > 2 && @active_blobs.length < 40
      min_blob
    end


    def set_table_properties ( configuration )
      table_properties = configuration[:table_ids_properties][@table_id]
      @blob_max_delay = (table_properties[:blob_max_delay] if table_properties) || configuration[:blob_max_delay]
      @event_separator = (table_properties[:event_separator] if table_properties) || configuration[:event_separator]

      if table_properties
        @data_field = table_properties[:data_field]
        @csv_map = table_properties[:csv_map]
        @csv_default_value = table_properties[:csv_default_value] || configuration[:csv_default_value]
        @csv_separator = table_properties[:csv_separator] || configuration[:csv_separator]
      end
    end


    def set_event_format_ext ( configuration )
      table_properties = configuration[:table_ids_properties][@table_id]
      if table_properties.nil?
        @event_format_ext = DEFAULT_EXT_EVENT_FORMAT_JSON

      elsif table_properties[:ext]
        @event_format_ext = table_properties[:ext]

      elsif table_properties[:csv_map]
        @event_format_ext = DEFAULT_EXT_EVENT_FORMAT_CSV

      else
        @event_format_ext = DEFAULT_EXT_EVENT_FORMAT_JSON
      end
    end

  end
end
