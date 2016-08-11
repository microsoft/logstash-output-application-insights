# encoding: utf-8
#-------------------------------------------------------------------------
# # Copyright (c) Microsoft and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#--------------------------------------------------------------------------

class LogStash::Outputs::Msai
  class Sub_channel

    public

    def initialize ( event_separator )
      @semaphore = Mutex.new
      @event_separator = event_separator
      @event_separator_bytesize = @event_separator.bytesize
      reset!
    end

    def reset!
      @bytesize = 0
      @reported_bytesize = 0
      @block_list = [  ]
      @block = nil
    end
    REPORT_BYTESIZE = 250 * 1024

    def << serialized_event
      @semaphore.synchronize {
        begin
          @block_list << ( @block = Block.new( @event_separator ) ) unless @block
          @block << serialized_event
          @block = nil if @block.is_full?

          @bytesize += ( serialized_event.bytesize + @event_separator_bytesize )
          unreported_bytesize = @bytesize - @reported_bytesize
          if unreported_bytesize > REPORT_BYTESIZE
            State.instance.inc_upload_bytesize( unreported_bytesize )
            @reported_bytesize = @bytesize
          end
        rescue BlockOverflowError
          @block = nil
          retry
        rescue BlockTooSmallError
          @@logger.error { "failed to receive event - " + "event too big" }
        end
      }
    end

    def get_block_list!
      @semaphore.synchronize {
        unreported_bytesize = @bytesize - @reported_bytesize
        State.instance.inc_upload_bytesize( unreported_bytesize ) if unreported_bytesize > 0
        block_list = @block_list
        reset!
        block_list
      }
    end

  end
end
