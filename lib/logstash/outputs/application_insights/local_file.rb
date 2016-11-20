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
  class Local_file

    attr_reader :bytesize
    attr_reader :events_count
    attr_reader :oldest_event_time

    attr_reader :file_size

    public

    def initialize ( filename, is_gzip_file )
      @file_name = filename
      @writer = write_file = File.new( @file_name, "wb" )
      @writer = Zlib::GzipWriter.new( write_file ) if is_gzip_file
      @read_file = nil
      @bytesize = 0
      @events_count = 0
      @first_block_number = nil
      @next_block_number = nil
      @next_event_count = nil
    end

    def seal
      @writer.close if @writer
      @writer = nil
    end

    def close_read
      @read_file.close if @read_file
      @read_file = nil
    end

    def open_read
      @read_file = File.new( @file_name, "rb" ) # File.new( @file_name, File::RDWR )
      @file_size =  @read_file.size
      @blocks_num = ( @file_size + BLOB_BLOCK_MAX_BYTESIZE - 1 ) / BLOB_BLOCK_MAX_BYTESIZE
      @events_per_block = @events_count / @blocks_num

      @next_event_count = @events_per_block + ( @events_count % @blocks_num )
      @first_block_number ||= Block.generate_block_numbers( @blocks_num )
      @next_block_number = @first_block_number
    end

    def get_next_block
      block = Block.new
      block.bytes = @read_file.read(BLOB_BLOCK_MAX_BYTESIZE)
      return nil if block.bytes.nil? || 0 == block.bytes.length

      block.bytesize = block.bytes.length
      State.instance.inc_upload_bytesize( block.bytesize )

      block.done_time = @done_time
      block.oldest_event_time = @oldest_event_time
      block.block_numbers = [ @next_block_number ]
      block.events_count = @next_event_count

      @next_event_count = @events_per_block
      @next_block_number += 1

      block
    end


    def << ( block )
      @bytesize += block.bytesize
      @events_count += block.events_count

      @writer.write( block.bytes )
      State.instance.dec_upload_bytesize( block.bytesize )

      @oldest_event_time = block.oldest_event_time if @oldest_event_time.nil? || block.oldest_event_time < @oldest_event_time
      @done_time = block.done_time if @done_time.nil? || block.done_time > @done_time
    end


    def dispose
      @bytesize = nil
      @events_count = nil
      @done_time = nil
      @oldest_event_time = nil
      seal
      close_read
      File.delete( @file_name ) if @file_name
      @file_name = nil
    end


    private

  end

end
