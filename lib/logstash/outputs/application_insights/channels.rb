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
      @tables = configuration[:tables]

      @flow_control = Flow_control.instance

      # launch tread that forward events from channels to azure storage
      periodic_forward_events
    end


    def receive ( event, encoded_event )
      if LogStash::SHUTDOWN == event
        @logger.info { "received a LogStash::SHUTDOWN event, start shutdown" }

      elsif LogStash::FLUSH == event
        @logger.info { "received a LogStash::FLUSH event, start shutdown" }
      end

      table_id = event[METADATA_FIELD_TABLE_ID] || event[FIELD_TABLE_ID] || @default_table_id
      intrumentation_key = event[METADATA_FIELD_INSTRUMENTATION_KEY] || event[FIELD_INSTRUMENTATION_KEY] || ( @tables[table_id][TABLE_PROPERTY_INSTRUMENTATION_KEY] if @tables[table_id] ) || @default_intrumentation_key

      @flow_control.pass_or_wait
      channel( intrumentation_key, table_id ) << event
    end


    def channel ( intrumentation_key, table_id )
      begin
        dispatch_channel( intrumentation_key, table_id )

      rescue NoChannelError
        begin
          create_channel( intrumentation_key, table_id )
        rescue ChannelExistError # can happen due to race conditions
          dispatch_channel( intrumentation_key, table_id )
        end
      end
    end


    def periodic_forward_events
      Thread.new do
        loop do
          sleep( 0.5 )
          channels = @create_semaphore.synchronize { @channels.dup }
          channels.each do |channel|
            channel.flush
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
        channel.close
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
