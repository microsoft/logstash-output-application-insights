# encoding: utf-8

require "logstash/outputs/base"
require "logstash/namespace"

require "stud/interval"

require "azure/storage"
require 'azure/storage/core/auth/shared_access_signature'

require 'azure/core/http/http_request'
require 'azure/core/http/http_response'

require 'rbconfig'
require 'faraday'
require 'openssl'
require "time"
require "json"
require "uri"
require "socket"

require "thread"
require "concurrent" # for atomic and thread safe operations
require "logger"
require "csv"

require "singleton"

# An msai output that does nothing.
class LogStash::Outputs::Msai < LogStash::Outputs::Base

  require "logstash/outputs/msai/utils" 
  require "logstash/outputs/msai/constants" 
  require "logstash/outputs/msai/config" 
  require "logstash/outputs/msai/blob" 
  autoload :Block, "logstash/outputs/msai/block"
  autoload :Storage_cleanup, "logstash/outputs/msai/storage_cleanup"

  autoload :Clients, "logstash/outputs/msai/clients" 
  autoload :Client, "logstash/outputs/msai/client" 

  autoload :Channels, "logstash/outputs/msai/channels"
  autoload :Channel, "logstash/outputs/msai/channel"
  autoload :Sub_channel, "logstash/outputs/msai/sub_channel" 
  autoload :State, "logstash/outputs/msai/state"
  autoload :Flow_control, "logstash/outputs/msai/flow_control"
  autoload :Shutdown, "logstash/outputs/msai/shutdown"

  require "logstash/outputs/msai/notify" 
  require "logstash/outputs/msai/exceptions" 
  require "logstash/outputs/msai/timer" 
  require "logstash/outputs/msai/multi_io_logger" 

  config_name "msai"
    
  default :codec, "json_lines" 
  
  # Array of pairs, storage_account_name and an array of acces_keys 
  # examples [ account1, key1 ]
  #          [ [account1, key1], [accoun2, key2], [account3, key3] ]
  #          [ [account1, [key11, key12]], [account2, key2], [account3, [key3] ]
  config :storage_account_name_key, :validate => :array, :required => true

  # prefix for the azure storage tables used by this Logstash instance
  # it is recommeded that each Logstash prefix have a unique prefix, to avoid
  # confusion and loss of tracking, although sharing tables won't damage proper execution
  # if not set, the machine name is used (not alphanumeric characters are removed, and converted downcase), if machine name available
  # string may contain only alphanumeric character, case sensitive, and must start with a letter 
  config :table_prefix, :validate => :string

  # prefix for the azure storage containers used by this Logstash instance
  # it is recommeded that each Logstash prefix have a unique prefix, to avoid
  # confusion and loss of tracking, although sharing containers won't damage proper execution
  # if not set, the machine name is used (not alphanumeric characters are removed, and converted downcase), if machine name available
  # string may contain only alphanumeric character and dash, double dash not allowed, case insesitive
  config :container_prefix, :validate => :string

  # prefix for the azure storage blobs used by this Logstash instance
  # Each Logstash prefix MUST have a unique prefix, to avoid
  # loss of data !!!
  # if not set, the machine name is used (not alphanumeric characters are removed, and converted downcase), if machine name available
  # string may include only characters that are allowed in any valid url
  config :blob_prefix, :validate => :string
  
  # Advanced, internal, should not be set, the only current valid value is "json"
  config :json_ext, :validate => :string

  # Advanced, internal, should not be set, the only current valid value is "csv"
  config :csv_ext, :validate => :string

  # Default Application Insights Analytics ikey
  # will be used only in case it is not specified as a schema property in schemas_properties
  # or as part of the event's fields or event's metadata fields
  config :ikey, :validate => :string

  # Default Application Insights Analytics ikey
  # will be used only in case it is not specified as part o
  # of the event's fields or event's metadata fields
  config :schema, :validate => :string

  # A hash of schemas, where each schema points to a set of properties
  # the properties are a hash, where the keys are are the properties
  # current supported properties per schema are:
  # ikey, ext, csv_map, csv_default_value, csv_separator, max_delay, event_separator, data_field
  # ikey, Application Insights Analytics ikey, will be used in case not specified in any of the event's fields or events's metadata fileds
  # data_field, specifies the data field that will contain the full serialize event (either as json or csv), 
  #             when specified, the ext property should be set either to csv or to json
  #             should not be specified together with csv_map property 
  # ext, blob extension, the only valid values are either csv or json, 
  #      should and must be specified only together with the data_field property
  # max_delay, maximum latency time, in seconds, since the time the event arrived till it should be commited in azure storage, and Application Insights is notified
  # event_separator, specifies the string that is used as a separator between events in the blob
  # csv_map, specifies the event fields that maps to the csv columns, based on their order
  #          if specified csv serialization will be used for this schema
  #          should not be specified together with data_field property 
  #          each csv_map field is a hash with 3 keys: name, type, and default. Only name is mandatory
  #               name - is the name of the event fleld that its value should be mapped to this columns
  #               type - is the type of this field: "string", "hash", "array", "number", "json", "boolean", "float", "integer", "dynamic", "datetime", "object"
  #               default - is the value to be used for this column, in case the field is missing in the event
  # csv_separator, specifies the string that is used as a separator between columns, 
  #                can be specified only together with csv_map
  # csv_default_value, specifies the string that is used as the value in a csv record, in case the field does not exist in the event
  #                can be specified only together with csv_map
  #
  # Example json schema
  #   schemas_properties => {"a679fbd2-702c-4c46-8548-80082c66ef28" => {"ikey" => "abee940b-e648-4242-b6b3-f2826667bf96", "max_delay" => 60} }
  # Example json schema, input in data_field
  #   {"ab6a3584-aef0-4a82-8725-2f2336e59f3e" => {"data_field" => "message". "ext" => "json"} }
  # Example csv schema, input in data_field
  #   {"ab6a3584-aef0-4a82-8725-2f2336e59f3e" => {"data_field" => "message". "ext" => "csv"} }
  # Example csv schema, input in event fields
  #   {"ab6a3584-aef0-4a82-8725-2f2336e59f3e" => { "csv_map" => [ {name => "Timestamp" type => datetime }, "Value", "Custom" ] } }

  config :schemas_properties, :validate => :hash, :default => {}


  # Advanced, internal, should not be set, the default is 50,000, 
  # azure storage maximum number of blocks per blob is 50,000
  config :blob_max_blocks, :validate => :number

  # Advanced, internal, should not be set, the default is 192 GB ( = 50,000 * 4 MB ) 
  # azure storage maximum number of blocks per blob is 192 GB ( = 50,000 * 4 MB ) 
  config :blob_max_bytesize, :validate => :number

  # Specifies, maximum number of events in one blob,
  # setting it low may reduce the latency, but reduce logstash performance
  # setting it high may increase latency to maximum delay, but logstash will be more efficient, and load on network will be lower
  config :blob_max_events, :validate => :number

  # Specifies maximum latency time, in seconds, since the time the event arrived 
  # till it is commited to azure storage, and Application Insights is notified
  # The total latency may be higher, as this is not the full ingestion flow 
  config :blob_max_delay, :validate => :number

  # Interval of time between retries due to IO failures
  config :io_retry_delay, :validate => :number

  # Number of retries on IO failures, before giving up, and move to available options
  config :io_max_retries, :validate => :number

  # Specifies the retention time of the blob in the container after it is notified to Application Insighta Analytics
  # Once the retention time expires, the blob is the deleted from container
  config :blob_retention_time, :validate => :number

  # Specifies the time Application Insights Analytics have access to the blob that are notifie
  # Blob access is limited with SAS URL
  config :blob_access_expiry_time, :validate => :number

  # Advanced, internal, should not be set, the default is \r\n, 
  # specifies the string that is used as a separator between events in the blob
  config :event_separator, :validate => :string

  # Advanced, internal, should not be set, the default is comma, 
  # specifies the string that is used as a separator between columns in a csv record
  config :csv_separator, :validate => :string

  # specifies the string that is used as the value in a csv record, in case the field does not exist in the event, the default is ""
  config :csv_default_value, :validate => :string

  # specifies the log level. valid values are: DEBUG, INFO, WARN, ERROR, FATAL, UNKNOWN
  config :logger_level, :validate => :string

  # Specifies the list of targets for the log. may include files, devices, "stdout: and "stderr"
  config :logger_files, :validate => :array

  # Advanced, internal, should not be set, the default is MSAI, 
  # Specifies the program name that will displayed in each log record
  config :logger_shift_progname, :validate => :string

  # Specifies when file logs are shifted. valid values are either an integer or "daily", "weekly" or "monthly"
  config :logger_shift_size

  # Specifies the shift age of a log
  config :logger_shift_age, :validate => :number

  # Specifies the time interval, between tests that check whether a stoarge account came back to life, 
  # after it stoped responding 
  config :resurrect_delay, :validate => :number

  # specifies the high water mark for the flow control, that used to avoid out of memory crash
  # once memory consumption reach reach the high water mark, the plugin will stop accepting events, till memory
  # is below the low water mark
  config :flow_control_suspend_bytes, :validate => :number

  # specifies the low water mark for the flow control, that used to avoid out of memory crash
  # once memory consumption reach the high water mark, the plugin will stop accepting events, till memory
  # is below the low water mark
  config :flow_control_resume_bytes, :validate => :number

  # specifies the amount of time the flow control suspend receiving event, to allow GC, and flush of event to Azure storage
  # before checking whether memory is below low water mark
  config :flow_control_delay, :validate => :number

  # File path of the CA file if having issue with SSL
  config :ca_file, :validate => :string

  # Advanced, internal, should not be set, the default is false
  # When set to true, notification is not sent to application insights, but behaves as if notified
  config :disable_notification, :validate => :boolean

  # Advanced, internal, should not be set, the default is false
  # When set to true, events are not uploaded, and blob not commited, but behaves as if uploaded and notified
  config :disable_blob_upload, :validate => :boolean

  # Advanced, internal, should not be set, the default is Application Insights production endpoint
  # when set notification are sent to an alternative endpoint, used for internal testing
  config :notification_endpoint, :validate => :string

  # Advanced, internal, should not be set, the only current valid value is 1
  config :notification_version, :validate => :number

  public

  def register

    # set configuration
    Config.validate_and_adjust_configuration( default_configuration )
    configuration = Config.current

    Multi_io_logger.config( configuration )

    # be careful don't use here @logger, as it will override Logstash @logger, and may create starnge behaviour
    @private_logger = configuration[:logger]

    @private_logger.info { "configuration: #{configuration}" }

    Timer.config( configuration )
    Blob.config( configuration )


    @shutdown = Shutdown.instance
    @channels = Channels.instance

    @storage_cleanup = Storage_cleanup.instance

    @private_logger.info { "plugin registered" }
    return "ok\n"
  end # def register
  

  def receive ( event )
    @channels.receive( event )
    return "ok\n"
  end

  def close
    @shutdown.submit
  end

  private

  # -----------------------------------------------


  def list_blob_names
    blob_names = Set.new []
    loop do
      continuation_token = NIL
      entries = @azure_blob.list_blobs(@container, { :timeout => 10, :marker => continuation_token})
      @@logger.debug { 'blob entries: #{entries}' }
      entries.each do |entry|
        @@logger.debug { 'blob entry name: #{entry.name}' }
        blob_names << entry.name
      end
      continuation_token = entries.continuation_token
      break if continuation_token.empty?
    end
    return blob_names
  end # def list_blobs


  def list_container_names
    container_names = Set.new []
    loop do
      continuation_token = NIL
      containers = @azure_blob.list_containers()
      @@logger.debug { 'containers: #{containers}' }
      containers.each do |container|
        @@logger.debug { 'container entry name:' + container.name }
        container_names << container.name
        upload(container.name, "blob-append-" + container.name, "test - " + container.name)
        blobs = @azure_blob.list_blobs(container.name)
        blobs.each do |blob|
          @@logger.debug { 'blob name: ' + blob.name }
        end
      end
      continuation_token = containers.continuation_token
      break if continuation_token.empty?
    end
    return container_names
  end # def list_blobs

  def create_container (container_name)
    begin
      @azure_blob.create_container(container_name)
    rescue
      @@logger.debug { $! }
    end
  end
end # class LogStash::Outputs::Msai

