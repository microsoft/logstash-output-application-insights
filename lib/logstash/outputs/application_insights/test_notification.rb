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
  class Test_notification < Blob


    def initialize
      # super first parameter must be nil. blob first parameter is channel, otherwise it will pass storage_account_name as channel
      super( nil )
      @storage_account_name = @configuration[:storage_account_name_key][0][0]
      @action = :test_notification
      @info = "#{@action}"
      @recoverable = [ :invalid_instrumentation_key, :invalid_table_id ]
      @force_client = true # to enable get a client even if all storage_accounts marked dead
      @container_name = "logstash-test-container"
      @blob_name = "logstash-test-blob"
      @table_id = GUID_NULL
      @instrumentation_key = GUID_NULL
    end

    def submit
      @max_tries = 1
      storage_io_block {
        if @recovery.nil?
          set_blob_sas_url
          payload = create_payload
          post_notification( @client.notifyClient, payload )
        end
      }
    end
  end
end
