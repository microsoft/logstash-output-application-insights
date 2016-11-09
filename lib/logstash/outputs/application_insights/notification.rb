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
  class Notification < Blob

    def initialize ( tuple )
      # super first parameter must be nil. blob first parameter is channel, otherwise it will pass storage_account_name as channel
      super( tuple )
    end

    # must return whether notification was successful or failed
    def notify
      @action = :notify
      @recoverable = [ :notify_failed_blob_not_accessible, :io_failure, :service_unavailable, :notification_process_down ]
      @force_client = true # to enable get a client even if all storage_accounts marked dead
      @info = "#{@action} #{@storage_account_name}/#{@container_name}/#{@blob_name}, events: #{@uploaded_events_count}, size: #{@uploaded_bytesize}, blocks: #{@uploaded_block_numbers}, delay: #{Time.now.utc - @oldest_event_time}"
      success = storage_io_block { submit }
      if success
        Telemetry.instance.track_event { { :name => "notified", :properties => context_to_table_entity } }
        state_table_update
      else
        notify_retry_later
      end
      success
    end

    private

    def submit
        set_blob_sas_url
        @logger.debug { "blob_sas_url: #{@blob_sas_url}" }
        payload = create_payload
        @logger.debug { "notification payload: #{payload}" }

        # assume that exceptions can be raised due to this method:
        post_notification( @client.notifyClient, payload ) unless @configuration[:disable_notification]

        @log_state = :notified
    end


    def set_blob_sas_url
      blob_url ="https://#{@storage_account_name}.blob.#{@configuration[:azure_storage_host_suffix]}/#{@container_name}/#{@blob_name}"
      options_and_constrains = {:permissions => "r", :resource => "b", :expiry => ( Time.now.utc + @configuration[:blob_access_expiry_time] ).iso8601 }
      @blob_sas_url = @client.storage_auth_sas.signed_uri( URI( blob_url ), options_and_constrains )
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


    def notify_retry_later
      if :notify_failed_blob_not_accessible == @recovery
        @sub_state = @recovery
        @storage_recovery.recover_later( context_to_tuple, :notify, @storage_account_name )

      elsif :invalid_instrumentation_key == @recovery || :invalid_table_id == @recovery
        @sub_state = @recovery
        Channels.instance.channel( @instrumentation_key, @table_id ).recover_later_notification( context_to_tuple )

      else
        if :notify_failed_blob_not_accessible == @sub_state
          @storage_recovery.recover_later( context_to_tuple, :notify, @storage_account_name )
        elsif :invalid_instrumentation_key == @sub_state || :invalid_table_id == @sub_state
          Channels.instance.channel( @instrumentation_key, @table_id ).recover_later_notification( context_to_tuple )
        else
          @notification_recovery.recover_later( context_to_tuple )
        end
      end
    end
  end
end
