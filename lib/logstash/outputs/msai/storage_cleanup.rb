# encoding: utf-8

class LogStash::Outputs::Msai
  class Storage_cleanup < Blob

    public

    def initialize
      super
      configuration = Config.current
      @logger = configuration[:logger]
      @container_prefix = configuration[:container_prefix]
      @retention_time = configuration[:blob_retention_time] + 24 * 60 * 60
      @not_notified_container = "failed-to-notify-blobs"
      # launch tread that cleans the storage
      periodic_storage_cleanup( configuration )
    end


    def periodic_storage_cleanup ( configuration )
      configuration[:storage_account_name_key].each do |storage_account_name, storage_account_keys|
        Thread.new( storage_account_name )  do |storage_account_name|
          loop do
            container_names = list_containers_to_cleanup( storage_account_name )
            container_names.each do |container_name|
              container_cleanup( storage_account_name, container_name )
            end
            sleep( 60 * 60 )
          end
        end
      end
    end


    # return list of containers ready to be cleaned up, return empty list in case failed to get list
    def list_containers_to_cleanup ( storage_account_name )
      continuation_token = nil
      container_names_to_delete = [  ]
      begin
        containers = list_container_names( storage_account_name, @container_prefix, continuation_token)
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
    def list_container_names ( storage_account_name, container_prefix = nil, token = nil )
      @storage_account_name = storage_account_name

      @action = :list_container_names
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable ]
      @info  = "#{@action} #{@storage_account_name}"

      containers = nil
      success =  storage_io_block( proc do |reason, e| end ) {
        options = { :metadata => true }
        options[:marker] = token if token
        options[:prefix] = container_prefix if container_prefix
        containers = @client.blobClient.list_containers( options )
      }
      containers
    end


    def container_cleanup ( storage_account_name, container_name )
      return unless copy_not_notified_blobs( storage_account_name, container_name )
      return unless delete_container_entities( storage_account_name, container_name )
      delete_container( storage_account_name, container_name )
    end


    # return true if all notified entities were copied
    def copy_not_notified_blobs( storage_account_name, container_name )
      pending = nil
      continuation_token = nil
      filter = "#{:container_name} eq '#{container_name}' and #{:log_state} ne '#{:notified}'"
      begin
        entities = log_to_table_query( storage_account_name, filter , continuation_token )
        return nil unless entities
        token = entities.continuation_token
        entities.each do |entity|
          blob_name = entity.properties[:blob_name.to_s]
          return nil unless ( status = not_notified_blob_copy_status( storage_account_name, blob_name ) )
          if :pending == status
            pending = true
          elsif :success != status
            return nil unless (status = copy_not_notified_blob( storage_account_name, container_name, blob_name ) )
            pending = true unless :success == status
          end
          @logger.warn { "copied blob: #{storage_account_name}/#{container_name}/#{blob_name} to #{@not_notified_container} container because cannot notify" } if :success == status
        end
      end while continuation_token
      pending.nil?
    end


    # return status or nil if failed
    def copy_not_notified_blob( storage_account_name, container_name, blob_name )
      @storage_account_name = storage_account_name

      @action = :copy_blob_to_not_notified_container
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable, :blob_exit, :create_container, :container_exist ]
      @info  = "#{@action} #{@storage_account_name}/#{container_name}/#{blob_name}"
      tuple = nil
      success =  storage_io_block( proc do |reason, e| end ) {
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
    def not_notified_blob_copy_status ( storage_account_name, blob_name )
      @storage_account_name = storage_account_name

      @action = :check_not_notified_blob_copy_status
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable, :create_resource, :create_container, :container_exist ]
      @info  = "#{@action} #{@storage_account_name}/#{@not_notified_container}/#{blob_name}"
      status = nil
      success =  storage_io_block( proc do |reason, e| end ) {
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
    def delete_container_entities( storage_account_name, container_name )
      continuation_token = nil
      filter = "#{:container_name} eq '#{container_name}'"
      begin
        entities = log_to_table_query( storage_account_name, filter , continuation_token )
        return nil unless entities
        token = entities.continuation_token
        entities.each do |entity|
          return nil unless log_to_table_delete( table_entity_to_tuple( entity.properties ) )
        end
      end while continuation_token
      true
    end


    # return true if container deleted
    def delete_container ( storage_account_name, container_name )
      @storage_account_name = storage_account_name

      @action = :delete_container
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable, :create_container ]
      @info  = "#{@action} #{@storage_account_name}/#{container_name}"

      success =  storage_io_block( proc do |reason, e| end ) {
        # delete container, if not found, skip
        containers = @client.blobClient.delete_container( container_name )  unless :create_container == @recovery
      }
      success
    end

    # singleton pattern
    public
    @@instance = Storage_cleanup.new
    def self.instance
      @@instance
    end
    private_class_method :new
  end
end
