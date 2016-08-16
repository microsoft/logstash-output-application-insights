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

require "logstash/outputs/base"
require "logstash/namespace"
require 'logstash-core/version'

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

require "application_insights"

class LogStash::Outputs::Application_insights < LogStash::Outputs::Base
  require "logstash/outputs/application_insights/version"
  require "logstash/outputs/application_insights/utils" 
  require "logstash/outputs/application_insights/constants" 
  require "logstash/outputs/application_insights/config" 
  require "logstash/outputs/application_insights/blob" 
  autoload :Block, "logstash/outputs/application_insights/block"
  autoload :Storage_cleanup, "logstash/outputs/application_insights/storage_cleanup"

  autoload :Clients, "logstash/outputs/application_insights/clients" 
  autoload :Client, "logstash/outputs/application_insights/client" 

  autoload :Channels, "logstash/outputs/application_insights/channels"
  autoload :Channel, "logstash/outputs/application_insights/channel"
  autoload :Sub_channel, "logstash/outputs/application_insights/sub_channel" 
  autoload :State, "logstash/outputs/application_insights/state"
  autoload :Flow_control, "logstash/outputs/application_insights/flow_control"
  autoload :Shutdown, "logstash/outputs/application_insights/shutdown"
  autoload :Telemetry, "logstash/outputs/application_insights/telemetry"

  require "logstash/outputs/application_insights/exceptions" 
  require "logstash/outputs/application_insights/timer" 
  require "logstash/outputs/application_insights/multi_io_logger" 

  config_name "application_insights"
    
  # default :codec, "json_lines" 
  
  # Array of pairs, storage_account_name and an array of acces_keys 
  # examples [ account1, key1 ]
  #          [ [account1, key1], [accoun2, key2], [account3, key3] ]
  #          [ [account1, [key11, key12]], [account2, key2], [account3, [key3] ]
  config :storage_account_name_key, :validate => :array

  # prefix for the azure storage tables used by this Logstash instance
  # it is recommeded that each Logstash prefix have a unique prefix, to avoid
  # confusion and loss of tracking, although sharing tables won't damage proper execution
  # if not set, the machine name is used (not alphanumeric characters are removed, and converted downcase), if machine name available
  # string may contain only alphanumeric character, case sensitive, and must start with a letter 
  config :azure_storage_table_prefix, :validate => :string

  # prefix for the azure storage containers used by this Logstash instance
  # it is recommeded that each Logstash prefix have a unique prefix, to avoid
  # confusion and loss of tracking, although sharing containers won't damage proper execution
  # if not set, the machine name is used (not alphanumeric characters are removed, and converted downcase), if machine name available
  # string may contain only alphanumeric character and dash, double dash not allowed, case insesitive
  config :azure_storage_container_prefix, :validate => :string

  # prefix for the azure storage blobs used by this Logstash instance
  # Each Logstash prefix MUST have a unique prefix, to avoid
  # loss of data !!!
  # if not set, the machine name is used (not alphanumeric characters are removed, and converted downcase), if machine name available
  # string may include only characters that are allowed in any valid url
  config :azure_storage_blob_prefix, :validate => :string
  
  # Default Application Insights Analytics intrumentation_key
  # will be used only in case it is not specified as a table_id property in tables
  # or as part of the event's fields or event's metadata fields
  config :intrumentation_key, :validate => :string

  # Default Application Insights Analytics table_id
  # will be used only in case it is not specified as part o
  # of the event's fields or event's metadata fields
  config :table_id, :validate => :string

  # specifies the list of the fields that will be filtered from the events, fields not specified will be ignored.
  # if not specified all fileds in events will be filtered, the order is kept, and is used for csv serialization
  config :table_columns, :validate => :array

  # if set to true events fields are refered as case insensitive, default case sensitive 
  config :case_insensitive_columns, :validate => :boolean

  # A hash of table_ids, where each table_id points to a set of properties
  # the properties are a hash, where the keys are are the properties
  # current supported properties per table_id are:
  # intrumentation_key, ext, table_columns, csv_default_value, csv_separator, max_delay, event_separator, data_field
  # intrumentation_key, Application Insights Analytics intrumentation_key, will be used in case not specified in any of the event's fields or events's metadata fileds
  # data_field, specifies the data field that may contain the full serialized event (either as json or csv), 
  #             when specified, the ext property should be set either to csv or to json (json is the default)
  #             if event. does not conatin the field, value will be created based on the fileds in the evnt, according to table_columns if configured, or all fileds in event
  #             if event contains this filed, and ext is csv
  #                  if value is a string, it will be used as is as the serialized event, without validating whether it is a csv string
  #                  if value is an array, it will be serialized as an array of csv columns
  #                  if value is a hash, it will be serialized based on table_columns to csv columns
  #             if event contains this filed, and ext is json
  #                  if value is a string, it will be used as is as the serialized event, without validating whether it is a json string
  #                  if value is a hash, it will be serialized to json, if fileds_map exit, it will be based on filds_map
  #                  if value is an array, it will be zipped with table_columns (if exist) and serialized to json
  # ext, blob extension, the only valid values are either csv or json, 
  #             should be set whenever the default json is not appropriate (.e, csv)
  # max_delay, maximum latency time, in seconds, since the time the event arrived till it should be commited in azure storage, and Application Insights is notified
  # event_separator, specifies the string that is used as a separator between events in the blob
  # table_columns, specifies the event fields that should be serialized, and their order (order is required for csv)
  #          if csv serialization will be used for this table_id
  #          each table_columns field is a hash with 3 keys: name, type, and default. Only name is mandatory
  #               name - is the name of the event fleld that its value should be mapped to this columns
  #               type - is the type of this field: "string", "hash", "array", "number", "json", "boolean", "float", "integer", "dynamic", "datetime", "object"
  #               default - is the value to be used for this column, in case the field is missing in the event
  # csv_separator, specifies the string that is used as a separator between columns, 
  #                can be specified only together with table_columns
  # csv_default_value, specifies the string that is used as the value in a csv record, in case the field does not exist in the event
  #                can be specified only together with table_columns
  #
  # Example json table_id
  #   tables => {"a679fbd2-702c-4c46-8548-80082c66ef28" => {"intrumentation_key" => "abee940b-e648-4242-b6b3-f2826667bf96", "max_delay" => 60} }
  # Example json table_id, input in data_field
  #   {"ab6a3584-aef0-4a82-8725-2f2336e59f3e" => {"data_field" => "message". "ext" => "json"} }
  # Example csv table_id, input in data_field
  #   {"ab6a3584-aef0-4a82-8725-2f2336e59f3e" => {"data_field" => "csv_message". "ext" => "csv"} }
  # Example csv table_id, input in event fields
  #   {"ab6a3584-aef0-4a82-8725-2f2336e59f3e" => { "ext" => "csv", "table_columns" => [ {name => "Timestamp" type => datetime }, "Value", "Custom" ] } }
  # Example csv table_id, input in event fields
  #   {"ab6a3584-aef0-4a82-8725-2f2336e59f3e" => { "ext" => "json", "table_columns" => [ "Timestamp", "Value", "Custom" ] } }

  config :tables, :validate => :hash, :default => {}

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

  # Advanced, internal, should not be set, the default is AI, 
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


  # When set to true, telemetry about the plugin, won't be sent to Application Insights
  config :disable_telemetry, :validate => :boolean

  # When set to true, storage cleanup won't be done by the plugin (should be done by some other means or by another Logstash process with this flag enabled)
  config :disable_cleanup, :validate => :boolean

  # When set to true, not notified blobs are deleted, if not set they are copied to the orphan-blobs container
  config :delete_not_notified_blobs, :validate => :boolean

  # When set to true, notified blobs records are save in table, as long as blobs are retained in their containers
  config :save_notified_blobs_records, :validate => :boolean

  # Advanced, internal, should not be set, the default is false
  # When set to true, notification is not sent to application insights, but behaves as if notified
  config :disable_notification, :validate => :boolean

  # Advanced, internal, should not be set, the default is false
  # When set to true, events are not uploaded, and blob not commited, but behaves as if uploaded and notified
  config :disable_blob_upload, :validate => :boolean

  # Advanced, internal, should not be set, the default is false
  # When set to true, process will stop if an unknown IO error is found
  config :stop_on_unknown_io_errors, :validate => :boolean

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

    @telemetry = Telemetry.instance
    configuration[:telemetry_channel] = @telemetry.telemetry_channel

    Timer.config( configuration )
    Blob.config( configuration )


    @shutdown = Shutdown.instance
    @channels = Channels.instance

    @storage_cleanup = Storage_cleanup.start

    @private_logger.info { "plugin registered" }

    # @codec.on_event do |event, encoded_event|
    #   @channels.receive( event, encoded_event )
    # end

    return "ok\n"
  end # def register
  

  def receive ( event )
    # @codec.encode( event )
    @channels.receive( event, nil )
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
end

