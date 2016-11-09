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
  class Storage_cleanup < Blob

    public

    def self.start
      configuration = Config.current
      @disable_cleanup = configuration[:disable_cleanup]
      @delete_not_notified_blobs = configuration[:delete_not_notified_blobs]

      unless @disable_cleanup
        @@list = [ ]
        configuration[:storage_account_name_key].each do |storage_account_name, storage_account_keys|
          @@list << Storage_cleanup.new( storage_account_name )
        end
      end
      private_class_method :new
    end


    def initialize ( storage_account_name )
      # super first parameter must be nil. blob first parameter is channel, otherwise it will pass storage_account_name as channel
      super( nil )
      @storage_account_name = storage_account_name
      @azure_storage_container_prefix = "#{AZURE_STORAGE_CONTAINER_LOGSTASH_PREFIX}#{@configuration[:azure_storage_container_prefix]}"
      @retention_time = @configuration[:blob_retention_time] + 24 * 60 * 60
      @not_notified_container = "#{AZURE_STORAGE_CONTAINER_LOGSTASH_PREFIX}#{@configuration[:azure_storage_container_prefix]}-#{AZURE_STORAGE_ORPHAN_BLOBS_CONTAINER_NAME}"
      # launch tread that cleans the storage
      periodic_storage_cleanup
    end


    def periodic_storage_cleanup
      Thread.new do
        loop do
          container_names = list_containers_to_cleanup
          container_names.each do |container_name|
            container_cleanup( container_name )
          end
          sleep( 60 * 60 )
        end
      end
    end


    # return list of containers ready to be cleaned up, return empty list in case failed to get list
    def list_containers_to_cleanup
      continuation_token = nil
      container_names_to_delete = [  ]
      begin
        containers = list_container_names( @azure_storage_container_prefix, continuation_token)
        break unless containers
        token = containers.continuation_token
        containers.each do |container|
          expiration_time = Time.parse( container.properties[:last_modified] ) + @retention_time
          container_names_to_delete << container.name if expiration_time <= Time.now.utc
        end
      end while continuation_token
      container_names_to_delete
    end


    # return blob containers
    def list_container_names ( azure_storage_container_prefix = nil, token = nil )
      @action = :list_container_names
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable ]
      @info  = "#{@action} #{@storage_account_name}"

      containers = nil
      success =  storage_io_block {
        options = { :metadata => true }
        options[:marker] = token if token
        options[:prefix] = azure_storage_container_prefix if azure_storage_container_prefix
        containers = @client.blobClient.list_containers( options )
      }
      containers
    end


    def container_cleanup ( container_name )
      unless  @delete_not_notified_blobs
        return unless copy_not_notified_blobs( container_name )
        return unless delete_container_entities( container_name )
      end
      delete_container( container_name )
    end


    # return true if all notified entities were copied
    def copy_not_notified_blobs( container_name )
      pending = nil
      continuation_token = nil
      filter = "#{:container_name} eq '#{container_name}' and #{:log_state} ne '#{:notified}'"
      begin
        entities = state_table_query( @storage_account_name, filter , continuation_token )
        return nil unless entities
        token = entities.continuation_token
        entities.each do |entity|
          blob_name = entity.properties[:blob_name.to_s]
          return nil unless ( status = not_notified_blob_copy_status( blob_name ) )
          if :pending == status
            pending = true
          elsif :success != status
            return nil unless (status = copy_not_notified_blob( container_name, blob_name ) )
            pending = true unless :success == status
          end
          @logger.warn { "copied blob: #{@storage_account_name}/#{container_name}/#{blob_name} to #{@not_notified_container} container because cannot notify" } if :success == status
        end
      end while continuation_token
      pending.nil?
    end


    # return status or nil if failed
    def copy_not_notified_blob( container_name, blob_name )
      @action = :copy_blob_to_not_notified_container
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable, :blob_exit, :create_container, :container_exist ]
      @info  = "#{@action} #{@storage_account_name}/#{container_name}/#{blob_name}"
      tuple = nil
      success =  storage_io_block {
        create_exist_recovery( :container, @not_notified_container ) { |name| @client.blobClient.create_container( name ) }
        if :blob_exit == @recovery
          tuple = ["", :pending]
        else
          tuple = @client.blobClient.copy_blob(@not_notified_container, blob_name, container_name, blob_name)
        end
      }
      tuple ? tuple[1].to_sym : nil
    end

    # return copy status, if failed return nil
    def not_notified_blob_copy_status ( blob_name )
      @action = :check_not_notified_blob_copy_status
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable, :create_resource, :create_container, :container_exist ]
      @info  = "#{@action} #{@storage_account_name}/#{@not_notified_container}/#{blob_name}"
      status = nil
      success =  storage_io_block {
        create_exist_recovery( :container, @not_notified_container ) { |name| @client.blobClient.create_container( name ) }
        if :create_resource == @recovery
          status = :not_started
        elsif
          result = @client.blobClient.get_blob_properties( @not_notified_container, blob_name )
          if result
            properties = result.properties
            status = ( properties[:copy_status] || :success).to_sym
          end
        end
      }
      status
    end

    # return true if all container entities were removed from log table
    def delete_container_entities( container_name )
      continuation_token = nil
      filter = "#{:container_name} eq '#{container_name}'"
      begin
        entities = state_table_query( @storage_account_name, filter , continuation_token )
        return nil unless entities
        token = entities.continuation_token
        entities.each do |entity|
          table_entity_to_context( entity.properties )
          return nil unless state_table_delete
        end
      end while continuation_token
      true
    end


    # return true if container deleted
    def delete_container ( container_name )
      @action = :delete_container
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable, :create_container ]
      @info  = "#{@action} #{@storage_account_name}/#{container_name}"

      success =  storage_io_block {
        # delete container, if not found, skip
        containers = @client.blobClient.delete_container( container_name )  unless :create_container == @recovery
      }
      success
    end

=begin
    # singleton pattern
    public
    @@instance = Storage_cleanup.new
    def self.instance
      @@instance
    end
    private_class_method :new
=end
  end
end
