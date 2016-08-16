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

class LogStash::Outputs::Application_insights
  class Timer
    public

    attr_accessor :state
    attr_reader :expiration
    attr_reader :object
    attr_reader :callback

    def self.config ( configuration )
      @@configuration = configuration
      @@logger = configuration[:logger]
      @@timers = []
      @@timers_modified = false
      @@timers_mutex = Mutex.new

      Thread.new do
        loop do
          sleep( 1 )

          curr_time = Time.now.utc
          timers_triggerd = [  ]

          @@timers_mutex.synchronize {
            @@timers.each do |timer|
              if :on == timer.state && curr_time >= timer.expiration
                timer.state = :trigger
                timers_triggerd << [ timer.object, timer.callback ]
              end
            end
          }

          timers_triggerd.each do |pair|
            (object, callback) = pair
            callback.call( object )
          end
        end
      end

    end

    def initialize
      @@timers_mutex.synchronize {
        @@timers << self
      }
      @state = :off
    end

    def set ( expiration, object, &callback )
      @@timers_mutex.synchronize {
        @@timers_modified= true
        @state = :on
        @object = object
        @expiration = expiration
        @callback = callback
      }
    end

    def cancel
      @@timers_mutex.synchronize {
        state = @state
        @state = :off
        @@timers_modified = true if :on == state
        state != :trigger
      }
    end

  end
end
