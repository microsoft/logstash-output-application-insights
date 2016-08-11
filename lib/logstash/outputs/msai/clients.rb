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
  class Clients

    public

    def initialize
      configuration = Config.current
      @resurrect_delay = configuration[:resurrect_delay]
      @storage_accounts = {  }
      @storage_account_names = [  ]
      @current_storage_account_names = [  ]
      @last_storage_account_key_index = 0
      @state_semaphore = Mutex.new
      create_blob_clients( configuration )
    end


    def active_storage_account ( storage_account_name , force )
      raise StorageAccountsOffError if 0 == @current_storage_account_names.length && force.nil?
      @storage_accounts[storage_account_name]
    end

    def create_blob_clients ( configuration )
      configuration[:storage_account_name_key].each do |storage_account_name, storage_account_keys|

        clients_Q = Queue.new
        @storage_accounts[storage_account_name] = { :keys => storage_account_keys, :clients_Q => clients_Q, :valid_index => 0, :state => :on, :off_reason => [] }
        40.times do
          # lazy creation, clients will be created when first needed
          clients_Q << [ nil, nil, nil]
        end
        @storage_account_names << storage_account_name
      end

      @current_storage_account_names = @storage_account_names.clone
    end


    def storage_account_state_on? ( storage_account_name = nil )
      if storage_account_name
        @storage_accounts[storage_account_name][:off_reason].empty?
      else
        @current_storage_account_names.length > 0
      end
    end

    def failed_storage_account( storage_account_name, io_failed_reason )
      storage_account = @storage_accounts[storage_account_name]
      @state_semaphore.synchronize {
        unless storage_account[:off_reason].include?( io_failed_reason )
          storage_account[:off_reason] << io_failed_reason
          if storage_account[:off_reason].length == 1
            current_storage_account_names = [  ]
            @current_storage_account_names.each do |account_name|
              current_storage_account_names << account_name unless account_name == storage_account_name
            end
            @current_storage_account_names = current_storage_account_names

            Thread.new( storage_account_name ) do |account_name|
              loop do
                sleep( @resurrect_delay )
                if Blob.new.test_storage( account_name )
                  @state_semaphore.synchronize {
                    storage_account = @storage_accounts[account_name]
                    storage_account[:off_reason] = [  ]
                    @current_storage_account_names << account_name
                  }
                  break
                end
              end
            end

          end
        end
      }
    end

    def get_random_active_storage( exclude )
      @state_semaphore.synchronize { storage_account_name = ( @current_storage_account_names - exclude ).sample if @current_storage_account_names.length > 0 }
    end

    public

    @@instance = Clients.new

    def self.instance
      @@instance
    end

    private_class_method :new
  end
end
