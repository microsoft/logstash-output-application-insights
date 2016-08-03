# encoding: utf-8

class LogStash::Outputs::Msai
  class Storage_cleanup < Blob

    public

    def initialize
      super
      configuration = Config.current
      @logger = configuration[:logger]
      @container_prefix = configuration[:container_prefix]
      # launch tread that cleans the storage
      periodic_storage_cleanup( configuration )
    end


    def periodic_storage_cleanup ( configuration )
      puts "max: #{@max_tries}"
      configuration[:storage_account_name_key].each do |storage_account_name, storage_account_keys|
        Thread.new( storage_account_name )  do |storage_account_name|
          loop do
            continuation_token = nil
            container_names_to_delete = [  ]
            retention_time = configuration[:blob_retention_time] + 24 * 60 * 60
            begin
              containers = list_container_names( storage_account_name, @container_prefix, continuation_token)
              break unless containers
              token = containers.continuation_token
              containers.each do |container|
                expiration_time = Time.parse( container.properties[:last_modified] ) + retention_time
                container_names_to_delete << container.name if expiration_time <= Time.now.utc
              end
            end while continuation_token

            container_names_to_delete.each do |container_name|
              continuation_token = nil
              filter = "#{:container_name} eq '#{container_name}' and #{:log_state} ne '#{:notified}'"
              blob_names_not_notified = [  ]
              begin
                entities = log_to_table_query( storage_account_name, filter , continuation_token )
                if entities
                  entities.each do |entity|
                    blob_names_not_notified << entity.properties[:blob_name]
                  end
                else
                  container_name = nil
                  break
                end
              end while continuation_token
              if blob_names_not_notified.empty?
                delete_container( storage_account_name, container_name ) if container_name
              else
                blob_names_not_notified.each do |blob_name|
                  @logger.warn { "cannot delete container, because blob #{blob_name} is not notified" }
                end
              end
            end
            sleep( 60 * 60 )
          end
        end
      end
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

    def delete_container ( storage_account_name, container_name )
      @storage_account_name = storage_account_name

      @action = :delete_container
      @recoverable = [ :invalid_storage_key, :io_failure, :service_unavailable ]
      @info  = "#{@action} #{@storage_account_name}/#{container_name}"

      success =  storage_io_block( proc do |reason, e| end ) {
        containers = @client.blobClient.delete_container( container_name )
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
