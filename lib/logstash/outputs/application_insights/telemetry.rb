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
  class Telemetry

    attr_reader :telemetry_channel

    public

    LOGSTASH_TELEMETRY_INSTRUMENTATION_KEY = "52fe6bb7-5528-4b4e-b2cc-12ef128633b4"

    def initialize
      configuration = Config.current
      @disable_telemetry = configuration[:disable_telemetry]
      unless @disable_telemetry
        @telemetry_channel = create_async_channel( LOGSTASH_TELEMETRY_INSTRUMENTATION_KEY )
        set_async_channel_properties( @telemetry_channel )
        set_channel_context( @telemetry_channel )
      end
    end


    def create_async_channel ( ikey )
      sender = ApplicationInsights::Channel::AsynchronousSender.new
      queue = ApplicationInsights::Channel::AsynchronousQueue.new( sender )
      channel = ApplicationInsights::Channel::TelemetryChannel.new( nil, queue )
      ApplicationInsights::TelemetryClient.new( ikey, channel )
    end

    def set_async_channel_properties ( tc )
      # flush telemetry if we have 10 or more telemetry items in our queue
      tc.channel.queue.max_queue_length = 10
      # send telemetry to the service in batches of 5
      tc.channel.sender.send_buffer_size = 5
      # the background worker thread will be active for 5 seconds before it shuts down. if
      # during this time items are picked up from the queue, the timer is reset.
      tc.channel.sender.send_time = 5
      # the background worker thread will poll the queue every 0.5 seconds for new items
      tc.channel.sender.send_interval = 0.5
    end

    def set_channel_context ( tc )
      # tc.context.application.id = 'logstash-output-Application-Insights plugin'
      tc.context.application.ver = VERSION
      tc.context.application.build = LOGSTASH_CORE_VERSION
      tc.context.device.id = Socket.gethostname.strip
      # tc.context.device.oem_name = 'Asus'
      # tc.context.device.model = 'X31A'
      tc.context.device.type = Utils.os
      # tc.context.user.id = 'santa@northpole.net'
    end

    def track_event ( name, options={} )
       @telemetry_channel.track_event( name, options ) unless @disable_telemetry
    end

    def track_metric ( name, value, options={} )
       @telemetry_channel.track_metric( name, value, options ) unless @disable_telemetry
    end

    def track_request (id, start_time, duration, response_code, success, options = {} )
      @telemetry_channel.track_request( id, start_time, duration, response_code, success, options ) unless @disable_telemetry
    end

    def flush
      @telemetry_channel.flush
    end

    public

    @@instance = Telemetry.new

    def self.instance
      @@instance
    end

    private_class_method :new
  end
end

