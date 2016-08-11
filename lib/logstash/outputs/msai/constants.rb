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

  def default_configuration
    {
      :notification_endpoint => @notification_endpoint || DEFAULT_NOTIFICATION_ENDPOINT,
      :notification_version => @notification_version || DEFAULT_NOTIFICATION_VERSION,

      :blob_prefix => @blob_prefix || DEFAULT_BLOB_PREFIX || Utils.to_storage_name( Socket.gethostname.strip ) || "",
      :container_prefix => @container_prefix || DEFAULT_CONTAINER_PREFIX || Utils.to_storage_name( Socket.gethostname.strip ) || "",
      :table_prefix => @table_prefix || DEFAULT_TABLE_PREFIX || Utils.to_storage_name( Socket.gethostname.strip ) || "",

      :storage_account_name_key => @storage_account_name_key,

      :event_separator => @event_separator || DEFAULT_EVENT_SEPARATOR,
      :csv_separator => @csv_separator || DEFAULT_CSV_SEPARATOR,
      :csv_default_value => @csv_default_value || DEFAULT_CSV_DEFAULT_VALUE,
      :table_ids_properties => @table_ids_properties || {  },

      :disable_notification => @disable_notification || DEFAULT_DISABLE_NOTIFICATION,
      :disable_blob_upload => @disable_blob_upload || DEFAULT_DISABLE_BLOB_UPLOAD,
      :stop_on_unknown_io_errors => @stop_on_unknown_io_errors || DEFAULT_STOP_ON_UNKNOWN_IO_ERRORS,

      :disable_telemetry => @disable_telemetry || DEFAULT_DISABLE_TELEMETRY,
      :disable_cleanup => @disable_cleanup || DEFAULT_DISABLE_CLEANUP,

      :intrumentation_key => @intrumentation_key || DEFAULT_INSTRUMENTATION_KEY,
      :table_id => @table_id || DEFAULT_TABLE_ID,

      :blob_max_bytesize => @blob_max_bytesize || DEFAULT_BLOB_MAX_BYTESIZE,
      :blob_max_events => @blob_max_events || DEFAULT_BLOB_MAX_EVENTS,
      :blob_max_delay => @blob_max_delay || DEFAULT_BLOB_MAX_DELAY,
      :blob_retention_time => @blob_retention_time || DEFAULT_BLOB_RETENTION_TIME,
      :blob_access_expiry_time => @blob_access_expiry_time || DEFAULT_BLOB_ACCESS_EXPIRY_TIME,

      :resurrect_delay => @resurrect_delay || DEFAULT_STORAGE_RESURRECT_DELAY,
      :io_retry_delay => @io_retry_delay || DEFAULT_IO_RETRY_DELAY,
      :io_max_retries => @io_max_retries || DEFAULT_IO_MAX_RETRIES,

      :logger_level => @logger_level || DEFAULT_LOG_LEVEL,
      :logger_files => @logger_files || DEFAULT_LOGGER_FILES,
      :logger_progname => @logger_progname || DEFAULT_LOGGER_PROGNAME,
      :logger_shift_size => @logger_shift_size || DEFAULT_LOGGER_SHIFT_SIZE,
      :logger_shift_age => @logger_shift_age || DEFAULT_LOGGER_SHIFT_AGE,

      :flow_control_suspend_bytes => @flow_control_suspend_bytes || DEFAULT_FLOW_CONTROL_SUSPEND_BYTES,
      :flow_control_resume_bytes => @flow_control_resume_bytes || DEFAULT_FLOW_CONTROL_RESUME_BYTES,
      :flow_control_delay => @flow_control_delay || DEFAULT_FLOW_CONTROL_DELAY,

      :ca_file => @ca_file, 

      :failed_on_action_nonrecoverable_Q => Queue.new
    }
  end

  VALID_UNDEFINDED = [ :disable_notification, :disable_blob_upload, :stop_on_unknown_io_errors, :disable_telemetry, :disable_cleanup, :ca_file, :storage_account_name_key ]

  GUID_NULL =       "00000000-0000-0000-0000-000000000000"
  INSTRUMENTATION_KEY_TEMPLATE =   "KKKKKKKK-KKKK-KKKK-KKKK-KKKKKKKKKKKK"
  TABLE_ID_TEMPLATE = "SSSSSSSS-SSSS-SSSS-SSSS-SSSSSSSSSSSS"

  # notification payload constants
  REQUEST_NAME = "Microsoft.ApplicationInsights.OpenSchema"
  BASE_DATA_REQUIRED_VERSION = "2"
  DATA_BASE_TYPE = "OpenSchemaData"

  # logger constants
  LOGGER_LEVEL_MAP = {:DEBUG => Logger::DEBUG, :INFO => Logger::INFO, :WARN => Logger::WARN, :ERROR => Logger::ERROR, :FATAL => Logger::FATAL, :UNKNOWN => Logger::UNKNOWN}

  BLOB_BLOCK_MAX_BYTESIZE = 4 * 1024 * 1024               # in bytes - 4 Mega bytes - blob limitation

  BLOB_MAX_BLOCKS = 50000                                 # in blocks - 50,0000 blocks - blob limitation

  MIN_BLOB_MAX_BYTESIZE = BLOB_BLOCK_MAX_BYTESIZE         # BLOB_BLOCK_MAX_BYTESIZE
  MAX_BLOB_MAX_BYTESIZE = BLOB_MAX_BLOCKS * BLOB_BLOCK_MAX_BYTESIZE   # 192 Giga  bytes

  MIN_BLOB_MAX_EVENTS = 1                                 # 256 Kilo events
  MAX_BLOB_MAX_EVENTS = 0                                 # No Limit

  MIN_BLOB_MAX_DELAY = 1                                  # in seconds - one second
  MAX_BLOB_MAX_DELAY = 24 * 3600                          # in seconds - one day

  MIN_BLOB_RETENTION_TIME = 60                            # in seconds - one minute
  MAX_BLOB_RETENTION_TIME = 60 * 60 * 24 * 365            # in seconds - one year

  MIN_BLOB_ACCESS_EXPIRY_TIME = 60                        # in seconds - one minute
  MAX_BLOB_ACCESS_EXPIRY_TIME = 60 * 60 * 24 * 365        # in seconds - one year

  MIN_STORAGE_RESURRECT_DELAY = 1                         # in seconds - one second
  MAX_STORAGE_RESURRECT_DELAY = 3600                      # in seconds - one hour

  MIN_LOGGER_SHIFT_SIZE = 1024                            # in bytes - 1 Kilo bytes
  MAX_LOGGER_SHIFT_SIZE = 1 * 1024 * 1024 * 1024          # in bytes - 1 Giga bytes

  MIN_LOGGER_SHIFT_AGE = 0                                # in retension version - no
  MAX_LOGGER_SHIFT_AGE = 100000                           # in retension version - almost limitless

  MIN_IO_RETRY_DELAY = 0                                  # in seconds
  MAX_IO_RETRY_DELAY = 300                                # in seconds - 5 minutes

  MIN_IO_MAX_RETRIES = 0
  MAX_IO_MAX_RETRIES = 3


  MIN_FLOW_CONTROL_SUSPEND_BYTES = 0                      # in bytes, 
  MAX_FLOW_CONTROL_SUSPEND_BYTES = 0                      # in bytes, 0 means no upper limit

  MIN_FLOW_CONTROL_RESUME_BYTES = 0                       # in bytes 
  MAX_FLOW_CONTROL_RESUME_BYTES = 0                       # in bytes, 0 means no upper limit

  MIN_FLOW_CONTROL_DELAY = 0.1                            # in seconds, 1 seconds, can be less than 1 seconds, like 0.5, 0.1
  MAX_FLOW_CONTROL_DELAY = 0                              # in seconds, 1 seconds, can be less than 1 seconds, like 0.5, 0.1

  METADATA_FIELD_INSTRUMENTATION_KEY = "[@metadata]intrumentation_key"
  METADATA_FIELD_TABLE_ID = "[@metadata]table_id"
  FIELD_INSTRUMENTATION_KEY = "intrumentation_key"
  FIELD_TABLE_ID = "table_id"

  STATE_TABLE_NAME = "BlobsState"

  CONTAINER_LOGSTASH_PREFIX = "logstash" # lower case only, dash allowed
  BLOB_LOGSTASH_PREFIX = "logstash"
  TABLE_LOGSTASH_PREFIX = "Logstash" # case sensitive, no dash

  TABLE_ID_PROPERTY_INSTRUMENTATION_KEY = "intrumentation_key"
  TABLE_ID_PROPERTY_EXT = "ext"
  TABLE_ID_PROPERTY_FIELDS_MAP = "fields_map"
  TABLE_ID_PROPERTY_CSV_DEFAULT_VALUE = "csv_default_value"
  TABLE_ID_PROPERTY_CSV_SEPARATOR = "csv_separator"
  TABLE_ID_PROPERTY_MAX_DELAY = "max_delay"
  TABLE_ID_PROPERTY_EVENT_SEPARATOR = "event_separator"
  TABLE_ID_PROPERTY_DATA_FIELD = "data_field"

  TABLE_ID_PROPERTY_FIELDS_MAP_NAME = "name"
  TABLE_ID_PROPERTY_FIELDS_MAP_TYPE = "type"
  TABLE_ID_PROPERTY_FIELDS_MAP_DEFAULT = "default"
  VALID_FIELDS_MAP_TYPES = [ "string", "hash", "array", "number", "json", "boolean", "float", "integer", "dynamic", "datetime", "object" ]
  
  VALID_LOGGER_SHIFT_AGES = [ "daily", "weekly", "monthly" ]

  EXT_EVENT_FORMAT_JSON = "json"
  EXT_EVENT_FORMAT_CSV = "csv"
  VALID_EXT_EVENT_FORMAT = [EXT_EVENT_FORMAT_JSON, EXT_EVENT_FORMAT_CSV ]

  DEFAULT_INSTRUMENTATION_KEY = GUID_NULL
  DEFAULT_TABLE_ID = GUID_NULL
  DEFAULT_EVENT_SEPARATOR = "\r\n"
  DEFAULT_CSV_SEPARATOR = ","
  DEFAULT_CSV_DEFAULT_VALUE = ""
  DEFAULT_BLOB_PREFIX = nil
  DEFAULT_CONTAINER_PREFIX = nil
  DEFAULT_TABLE_PREFIX = nil
  DEFAULT_JSON_EXT = "json"
  DEFAULT_CSV_EXT = "csv"

  DEFAULT_BLOB_MAX_BYTESIZE = 1 * 1024 * 1024 * 1024
  DEFAULT_BLOB_MAX_EVENTS = 256 * 1024                 # 256 Kilo events

  DEFAULT_BLOB_MAX_DELAY = 60                         # in seconds
  DEFAULT_BLOB_RETENTION_TIME = 60 * 60 * 24 * 7      # in seconds - one week
  DEFAULT_BLOB_ACCESS_EXPIRY_TIME = 60 * 60 * 24 * 1  # in seconds - one day
  DEFAULT_STORAGE_RESURRECT_DELAY = 10
  DEFAULT_NOTIFICATION_ENDPOINT = "https://dc.services.visualstudio.com/v2/track"
  DEFAULT_NOTIFICATION_VERSION = 1
  DEFAULT_DISABLE_NOTIFICATION = false
  DEFAULT_DISABLE_BLOB_UPLOAD = false
  DEFAULT_STOP_ON_UNKNOWN_IO_ERRORS = false
  DEFAULT_DISABLE_TELEMETRY = false
  DEFAULT_DISABLE_CLEANUP = false
  DEFAULT_LOGGER_FILES = [ "logstash-output-msai.log" ]
  DEFAULT_LOG_LEVEL = "INFO"
  DEFAULT_LOGGER_PROGNAME = "MS-AI"
  DEFAULT_LOGGER_SHIFT_AGE = "daily"                  # daily back retension
  DEFAULT_LOGGER_SHIFT_SIZE = 1024 * 1024             # in bytes - one Mega bytes
  DEFAULT_IO_RETRY_DELAY = 10                         # in seconds
  DEFAULT_IO_MAX_RETRIES = 2

  DEFAULT_FLOW_CONTROL_SUSPEND_BYTES = 50 *1024 * 1024        # high water mark, -in bytes, based on my laptop experience, without Java to break to NO MEMORY error
  DEFAULT_FLOW_CONTROL_RESUME_BYTES = 40 *1024 * 1024         # low water mark, 
  DEFAULT_FLOW_CONTROL_DELAY = 1                          # in seconds, 1 seconds, can be less than 1 seconds, like 0.5, 0.1
end
