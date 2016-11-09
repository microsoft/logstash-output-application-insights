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
  class Context

    def initialize ( tuple )
      if tuple
        tuple_to_context( tuple )
      else
        clear_context
      end
    end

    def clear_context
      @start_time = Time.now.utc
      @action = nil
      @instrumentation_key = nil
      @table_id = nil
      @storage_account_name = nil
      @container_name = nil
      @blob_name = nil
      @uploaded_block_ids = [  ]
      @uploaded_block_numbers = [  ]
      @uploaded_events_count = 0
      @uploaded_bytesize = 0
      @oldest_event_time = nil
      @event_format = nil
      @blob_max_delay = nil
      @log_state = nil
      @sub_state = :none
      @file_size = 0
    end

    def table_entity_to_tuple( options = {} )
      [ options[:start_time.to_s] || Time.now.utc, options[:action.to_s], options[:instrumentation_key.to_s], options[:table_id.to_s],
        options[:storage_account_name.to_s], options[:container_name.to_s], options[:blob_name.to_s],
        eval( options[:uploaded_block_ids.to_s] ), eval( options[:uploaded_block_numbers.to_s] ),
        options[:uploaded_events_count.to_s] || 0, options[:uploaded_bytesize.to_s] || 0, options[:oldest_event_time.to_s] || Time.now.utc,
        options[:event_format.to_s], options[:blob_max_delay.to_s] || 0,
        options[:log_state.to_s].to_sym, (options[:sub_state.to_s] || :none).to_sym, options[:file_size.to_s] || 0
      ]
    end

    def table_entity_to_context ( table_entity )
      tuple_to_context( table_entity_to_tuple( table_entity ) )
    end


    def context_to_tuple
      [ @start_time || Time.now.utc, @action, @instrumentation_key, @table_id,
        @storage_account_name, @container_name, @blob_name,
        @uploaded_block_ids, @uploaded_block_numbers,
        @uploaded_events_count, @uploaded_bytesize, @oldest_event_time,
        @event_format, @blob_max_delay,
        @log_state, @sub_state, @file_size
      ]
    end

    def tuple_to_context ( tuple )
      ( @start_time, @action, @instrumentation_key, @table_id,
        @storage_account_name, @container_name, @blob_name,
        @uploaded_block_ids, @uploaded_block_numbers,
        @uploaded_events_count, @uploaded_bytesize, @oldest_event_time,
        @event_format, @blob_max_delay,
        @log_state, @sub_state, @file_size ) = tuple
    end

    def context_to_table_entity
      { :start_time => @start_time, :instrumentation_key => @instrumentation_key, :table_id => @table_id,
        :storage_account_name => @storage_account_name, :container_name => @container_name, :blob_name => @blob_name,
        :uploaded_block_ids => @uploaded_block_ids.to_s, :uploaded_block_numbers => @uploaded_block_numbers.to_s,
        :uploaded_events_count => @uploaded_events_count, :uploaded_bytesize => @uploaded_bytesize, :oldest_event_time => @oldest_event_time,
        :log_state => @log_state, :sub_state => @sub_state, :file_size => @file_size
      }
    end

  end
end
