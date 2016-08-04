# encoding: utf-8

class LogStash::Outputs::Msai
  class Channels

    public

    def initialize
      configuration = Config.current

      @logger = configuration[:logger]

      @intrumentation_key_table_id_db = {}
      @channels = [  ]
      @create_semaphore = Mutex.new

      @default_intrumentation_key = configuration[:intrumentation_key]
      @default_table_id = configuration[:table_id]
      @table_ids_properties = configuration[:table_ids_properties]

      @flow_control = Flow_control.instance

      # launch tread that forward events from channels to azure storage
      periodic_forward_events
    end


    def receive ( event )
      if LogStash::SHUTDOWN == event
        @logger.info { "received a LogStash::SHUTDOWN event, start shutdown" }

      elsif LogStash::FLUSH == event
        @logger.info { "received a LogStash::FLUSH event, start shutdown" }

      else
        table_id = event[METADATA_FIELD_TABLE_ID] || event[FIELD_TABLE_ID] || @default_table_id
        intrumentation_key = event[METADATA_FIELD_INSTRUMENTATION_KEY] || event[FIELD_INSTRUMENTATION_KEY] || ( @table_ids_properties[table_id][TABLE_ID_PROPERTY_INSTRUMENTATION_KEY] if @table_ids_properties[table_id] ) || @default_intrumentation_key

        begin
          channel = dispatch_channel( intrumentation_key, table_id )

        rescue NoChannelError
          begin
            channel = create_channel( intrumentation_key, table_id )
          rescue ChannelExistError # can happen due to race conditions
            channel = dispatch_channel( intrumentation_key, table_id )
          end
        end

        @flow_control.pass_or_wait
        channel << event
      end
    end


    def periodic_forward_events
      Thread.new do
        loop do
          sleep( 0.5 )
          channels = @create_semaphore.synchronize { @channels.dup }
          channels.each do |channel|
            block_list = channel.collect_blocks
            channel.enqueue_blocks( block_list )
          end
        end
      end
    end

    private

    # return channel
    def dispatch_channel ( intrumentation_key, table_id )
      begin
        channel = @intrumentation_key_table_id_db[intrumentation_key][table_id]
        channel.intrumentation_key     # don't remove it, it is to emit an exception in case channel not created yet'
        channel
      rescue => e
        raise NoChannelError if @intrumentation_key_table_id_db[intrumentation_key].nil? || @intrumentation_key_table_id_db[intrumentation_key][table_id].nil?
        @logger.error { "Channel dispatch failed - error: #{e.inspect}" }
        raise e
      end 
    end


    # return channel
    def create_channel ( intrumentation_key, table_id )
      @create_semaphore.synchronize {      
        raise ChannelExistError if @intrumentation_key_table_id_db[intrumentation_key] && @intrumentation_key_table_id_db[intrumentation_key][table_id]
        @intrumentation_key_table_id_db[intrumentation_key] ||= {}
        channel = Channel.new( intrumentation_key, table_id )
        @intrumentation_key_table_id_db[intrumentation_key][table_id] = channel
        @channels << channel
        channel
      }
    end

    public

    def close
      @channels.each do |channel|
        channel.close_active_blobs
      end
    end

    def mark_invalid_intrumentation_key ( intrumentation_key )
      # TODO should go to lost and found container
    end

    def mark_invalid_table_id ( table_id )
      # TODO should go to lost and found container
    end

    public

    @@instance = Channels.new
    def self.instance
      @@instance
    end

    private_class_method :new
  end
end
