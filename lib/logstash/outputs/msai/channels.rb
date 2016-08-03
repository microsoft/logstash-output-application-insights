# encoding: utf-8

class LogStash::Outputs::Msai
  class Channels

    public

    def initialize
      configuration = Config.current

      @logger = configuration[:logger]

      @ikey_schema_db = {}
      @channels = [  ]
      @create_semaphore = Mutex.new

      @default_ikey = configuration[:ikey]
      @default_schema = configuration[:schema]
      @schemas_properties = configuration[:schemas_properties]

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
        schema = event[METADATA_FIELD_SCHEMA] || event[FIELD_SCHEMA] || @default_schema
        ikey = event[METADATA_FIELD_IKEY] || event[FIELD_IKEY] || ( @schemas_properties[schema][SCHEMA_PROPERTY_IKEY] if @schemas_properties[schema] ) || @default_ikey

        begin
          channel = dispatch_channel( ikey, schema )

        rescue NoChannelError
          begin
            channel = create_channel( ikey, schema )
          rescue ChannelExistError # can happen due to race conditions
            channel = dispatch_channel( ikey, schema )
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
    def dispatch_channel ( ikey, schema )
      begin
        channel = @ikey_schema_db[ikey][schema]
        channel.ikey     # don't remove it, it is to emit an exception in case channel not created yet'
        channel
      rescue => e
        raise NoChannelError if @ikey_schema_db[ikey].nil? || @ikey_schema_db[ikey][schema].nil?
        @logger.error { "Channel dispatch failed - error: #{e.inspect}" }
        raise e
      end 
    end


    # return channel
    def create_channel ( ikey, schema )
      @create_semaphore.synchronize {      
        raise ChannelExistError if @ikey_schema_db[ikey] && @ikey_schema_db[ikey][schema]
        @ikey_schema_db[ikey] ||= {}
        channel = Channel.new( ikey, schema )
        @ikey_schema_db[ikey][schema] = channel
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

    def mark_invalid_ikey ( ikey )
      # TODO should go to lost and found container
    end

    def mark_invalid_schema ( schema )
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
