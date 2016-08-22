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
  class Block

    attr_reader :bytes
    attr_reader :buffer
    attr_reader :bytesize
    attr_reader :events_count
    attr_reader :block_numbers
    attr_reader :done_time
    attr_reader :oldest_event_time


    public

    @@Block_number = 0
    @@semaphore = Mutex.new

    def self.generate_block_number
      @@semaphore.synchronize { @@Block_number = ( @@Block_number + 1 ) % 1000000 }
    end



    def initialize ( event_separator )
      @buffer = [  ]
      @bytesize = 0
      @events_count = 0
      @event_separator = event_separator
      @event_separator_bytesize = @event_separator.bytesize
      @block_numbers = nil
    end

    # concatenate two blocks into one
    def concat ( other )
      if @bytesize + other.bytesize <= BLOB_BLOCK_MAX_BYTESIZE
        if @block_numbers
          @block_numbers.concat( other.block_numbers ) if @block_numbers
          @bytes += other.bytes
          @done_time = other.done_time if other.done_time > @done_time
        else
          @buffer.concat( other.buffer )
        end
        @events_count += other.events_count
        @oldest_event_time = other.oldest_event_time if other.oldest_event_time < @oldest_event_time
        @bytesize += other.bytesize
      end
    end
  
    def << (data)
      @bytesize += data.bytesize + @event_separator_bytesize

      # if first data, it will accept even it overflows
      if is_overflowed? && @events_count > 0
        @bytesize -= data.bytesize + @event_separator_bytesize
        raise BlockTooSmallError if is_empty?
        raise BlockOverflowError
      end

      @oldest_event_time ||= Time.now.utc
      @events_count += 1
      @buffer << data
    end

    def dispose
      @bytes = nil
      @buffer = nil
      @bytesize = nil
      @events_count = nil
      @done_time = nil
      @oldest_event_time = nil
      @block_numbers = nil
    end

    def seal
      @block_numbers = [ Block.generate_block_number ]
      @done_time = Time.now.utc
      @buffer << "" # required to add eol after last event
      @bytes = @buffer.join( @event_separator )
      @buffer = nil # release the memory of the array
    end

    def is_full?
      @bytesize >= BLOB_BLOCK_MAX_BYTESIZE
    end
    
    private

    def is_overflowed?
      @bytesize > BLOB_BLOCK_MAX_BYTESIZE
    end
    
    def is_empty?
      @bytesize <= 0
    end

  end
end
