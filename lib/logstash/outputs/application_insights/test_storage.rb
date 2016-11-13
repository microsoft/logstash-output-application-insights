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
  class Test_storage < Blob


    def initialize ( storage_account_name )
      # super first parameter must be nil. blob first parameter is channel, otherwise it will pass storage_account_name as channel
      super( nil )
      @storage_account_name = storage_account_name
      @test_storage_container = @configuration[:test_storage_container]
      @test_storage_table = @configuration[:test_storage_table]
      @action = :test_storage
      @base_info = "#{@action} #{@storage_account_name}"
      @recoverable = [ :invalid_storage_key, :container_exist, :create_container, :table_exist, :create_table ]
      @force_client = true # to enable get a client even if all storage_accounts marked dead
    end

    def test
      test_container
    end

    def test_container
      @max_tries = 1
      @info = "#{@base_info} - create #{@test_storage_container} container"
      storage_io_block {
        if @recovery.nil? || :invalid_storage_key == @recovery
          @client.blobClient.create_container( @test_storage_container ) unless @configuration[:disable_blob_upload]
        end
      }
    end

    def test_table
      @max_tries = 1
      @info = "#{@base_info} - create #{@test_storage_table} table"
      storage_io_block {
        if @recovery.nil? || :invalid_storage_key == @recovery
          @client.tableClient.create_table( @test_storage_table ) unless @configuration[:disable_blob_upload]
        end
      }
    end
  end
end
