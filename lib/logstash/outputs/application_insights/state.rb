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
  class State

    public

    def initialize
      @bytes_in_memory = Concurrent::AtomicFixnum.new(0)
      @pending_commits = Concurrent::AtomicFixnum.new(0)
      @pending_notifications = Concurrent::AtomicFixnum.new(0)
    end


    def bytes_in_memory
      @bytes_in_memory.value
    end


    def pending_commits
      @pending_commits.value
    end


    def pending_notifications
      @pending_notifications.value
    end


    def inc_upload_bytesize ( bytesize )
      @bytes_in_memory.increment( bytesize )
    end


    def dec_upload_bytesize ( bytesize )
      @bytes_in_memory.decrement( bytesize )
    end


    def inc_pending_commits
      @pending_commits.increment
    end


    def dec_pending_commits
      @pending_commits.decrement
    end


    def inc_pending_notifications
      @pending_notifications.increment
    end


    def dec_pending_notifications
      @pending_notifications.decrement
    end

    public

    @@instance = State.new

    def self.instance
      @@instance
    end

    private_class_method :new
  end
end

