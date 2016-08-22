# Microsoft Application Insights Output Plugin for Logstash

[![GitHub version](https://badge.fury.io/gh/microsoft%2Flogstash-output-application-insights.svg)](https://badge.fury.io/gh/microsoft%2Flogstash-output-application-insights)

This project is a plugin for [Logstash](https://github.com/elastic/logstash).

# Plugin Features

# Supported Logstash Versions

* Logstash 2.3.2
* Logstash 2.3.4

Note: 

* x64 Ruby for Windows is known to have some compatibility issues.
* the plugin depends on azure-storage that depends on gem nokogiri, which doesn't support Ruby 2.2+ on Windows.

# Running Plugin in Logstash

## Option1: Run in a local Logstash clone

- Edit Logstash `Gemfile` and add the logstash-output-application-insights plugin path:
```ruby
gem "logstash-output-application-insights", :path => "/../logstash-output-application-insights"
```
- Install plugin
```sh
# Logstash 2.3 and higher
bin/logstash-plugin install --no-verify

```
- Run Logstash with plugin
```sh
bin/logstash -f 'file://localhost/../your-config-file'
```
## Option2:  Run in an installed Logstash

You can use the same method as in local Logstash clone to run the plugin in an installed Logstash by editing its `Gemfile` and pointing the `:path` to the local plugin directory or you can build the gem and install it using:

- Build your plugin gem
```sh
gem build logstash-output-application-insights.gemspec
```
- Install the plugin from the Logstash home
```sh
bin/logstash-plugin install --no-verify

```
- Start Logstash and use the plugin

## Plugin configuration parameters

### storage_account_name_key
Array of pairs, storage_account_name and an array of acces_keys. No default
At least one pair is required. If not defined, values will be taken (if exist) from Environment Variable: AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_ACCESS_KEY
examples:
```ruby
storage_account_name_key => [ "my-storage-account", "pfrYTwPgKyYNfKBY2QdF+v5sbgx8/eAQp+FFkGpPBnkMDE1k+ZNK3r3qIPqqw8UsOIUqaF3dXBdPDouGJuxNXQ==" ]

storage_account_name_key => [ ["my-storage-account1", "key1"], "my-storage-account2", "key2"], ["my-storage-account3", "key3"] ]

storage_account_name_key => [ ["my-storage-account1", ["key11", "key12"]], ["my-storage-account1", "key2"], ["my-storage-account1", ["key3"] ]
```

### azure_storage_table_prefix
A prefix for the azure storage tables name used by this Logstash instance. Default host name
It is recommeded that each Logstash instance have a unique prefix, to avoid confusion and loss of tracking, although sharing tables won't damage proper execution.
If not set, the host name is used (not alphanumeric characters are removed, and converted downcase), if host name available.
The prefix string may contain only alphanumeric characters, it is case sensitive, and must start with a letter 
example:
```ruby
azure_storage_table_prefix => "myprefix"
```

### azure_storage_container_prefix
A prefix for the azure storage containers name used by this Logstash instance. Default host name
It is recommeded that each Logstash prefix have a unique prefix, to avoid confusion and loss of tracking, although sharing containers won't damage proper execution.
if not set, the host name is used (not alphanumeric characters are removed, and converted downcase), if host name available.
The prefix string may contain only alphanumeric characters and dash, double dash are not allowed, it is case insesitive.
example:
```ruby
azure_storage_container_prefix => "myprefix"
```

### azure_storage_blob_prefix
A prefix for the azure storage blobs name used by this Logstash instance. Default host name
Each Logstash prefix MUST have a unique prefix, to avoid loss of data !!!
If not set, the host name is used (not alphanumeric characters are removed, and converted downcase), if host name available
string may include only characters that are allowed in any valid url
example:
```ruby
azure_storage_blob_prefix => "myprefix"
```

### intrumentation_key
Default Application Insights Analytics intrumentation_key. No default
It will be used only in case the key is not specified in the tables property associated to a table_id, or as field or metadata fields in the event
example:
```ruby
intrumentation_key => "5A6714A3-EC7B-4999-AB96-232F1DA92059"
```

### table_id
Default Application Insights Analytics table_id. No default
Will be used only in case it is not specified as field or metadata fields in the event
example:
```ruby
table_id => "C24394E1-F077-420E-8A25-EF6FDF045938"
```

### table_columns
Specifies the list of the fields that will be filtered from the event, fields not specified will be ignored. No Default (event all fields)
If not specified all fileds in events will be filtered, the order is kept. The order is essential in case of CSV serialization.
example:
```ruby
table_columns => [ "EventLogID", "AppName", "EnvironmentName", "ActivityID", "EventID", "Severity", "Title" ]
```

### case_insensitive_columns
If set to true, events fields are refered as case insensitive. Default false (case sensitive)
example:
```ruby
case_insensitive_columns => true
```

### blob_max_bytesize
Advanced, internal, should not be set. Default 1 GB.
Azure storage maximum bytesize is 192 GB ( = 50,000 * 4 MB ) 
example:
```ruby
blob_max_bytesize => 4000000000
```

### blob_max_events
Specifies, maximum number of events in one blob. Default 256,000 events
Setting it too low may improve latency, but will reduce ingestion performance
Setting it too high may damage latency up to maximum delay, but ingestion will be more efficient, and load on network will be lower
example:
```ruby
blob_max_events => 1000000
```

### blob_max_delay
Specifies maximum latency time, in seconds. Defualt 60 seconds
The latency time is measured since the time an event arrived till it is commited to azure storage, and Application Insights is notified.
The total latency time may be higher, as this is not the full ingestion flow 
example:
```ruby
blob_max_delay => 3600
```

### blob_serialization
Specifies the blob serialziation to create. Default "json"
currently 2 types are supported "csv" and "json""
example:
```ruby
blob_serialization => "json""
```

### io_retry_delay
Interval of time between retries due to IO failures
example:
```ruby
io_retry_delay => 0.5
```

### io_max_retries
Number of retries on IO failures, before giving up, and move to available options
example:
```ruby
io_max_retries => 3
```

### blob_retention_time
Specifies the retention time of the blob in the container after it is notified to Application Insighta Analytics. Dfeauly 604,800 seconds (1 week)
Once the retention time expires, the blob is the deleted from container
example:
```ruby
blob_retention_time => 604800
```

### blob_access_expiry_time
Specifies the time Application Insights Analytics have access to the blob that are notifie. Default 86,400 seconds ( 1 day)
Blob access is limited with SAS URL
example:
```ruby
blob_retention_time => 604800
```

### csv_default_value
Specifies the string that is used as the value in a csv record, in case the field does not exist in the event. Default ""
example:
```ruby
csv_default_value => "-"
```

### serialized_event_field
Specifies a serialized event field name, that if exist in current event, its value as is will be taken as the serialized event. No Default
example:
```ruby
serialized_event_field => "serializedMessage"
```


### logger_level
Specifies the log level. valid values are: DEBUG, INFO, WARN, ERROR, FATAL, UNKNOWN. Default "INFO"
example:
```ruby
logger_level => "INFO"
```

### logger_files
Specifies the list of targets for the log. may include files, devices, "stdout" and "stderr". Default "logstash-output-application-insights.log"
example:
```ruby
csv_default_value => [ "c:/logstash/dev/runtime/log/logstash-output-application-insights.log", "stdout" ]
```

### logger_progname
Specifies the program name that will displayed in each log record. Default "AI"
Should be modified only in case there is another plugin with the same program name
example:
```ruby
logger_progname => "MSAI"
```

### logger_shift_size
Specifies maximum logfile size. No Default (no size limit)
Only applies when shift age is a number !!!
Not supported in Windows !!!
example (1 MB):
```ruby
logger_shift_size => 1048576
```

### logger_shift_age
Specifies Number of old logfiles to keep, or frequency of rotation (daily, weekly or monthly). No default (never)
Not supported in Windows !!!
examples:
```ruby
logger_shift_age => weekly
```

```ruby
logger_shift_age => 5
```

### resurrect_delay
Specifies the time interval, between tests that check whether a stoarge account came back to life, after it stoped responding. Default 10 seconds
example (half second):
```ruby
flow_control_delay => 0.5
```

### flow_control_suspend_bytes
Specifies the high water mark for the flow control, that is used to avoid out of memory crash. Default 52,428,800 Bytes (50 MB)
Once the memory consumption reach the high water mark, the plugin will stop accepting events, till memory is below the low water mark
example (200 MB):
```ruby
flow_control_suspend_bytes => 209715200
```

### flow_control_resume_bytes
Specifies the low water mark for the flow control, that is used to avoid out of memory crash. Default 41,820,160 Bytes (40 MB)
Once memory consumption reach the high water mark, the plugin will stop accepting events, till memory is below the low water mark
example (10 MB):
```ruby
flow_control_resume_bytes => 10455040
```

### flow_control_delay
Specifies the amount of time the flow control suspend receiving event. Default 1 second
It is to allow GC, and flush of event to Azure storage before checking whether memory is below low water mark
example (half second):
```ruby
flow_control_delay => 0.5
```

### ca_file
File path of the CA file, required only if having issue with SSL (see OpenSSL). No default
example:
```ruby
ca_file => "/path/to/cafile.crt"
```

### disable_telemetry
When set to true, telemetry about the plugin, won't be sent to Application Insights. Deafult false
example:
```ruby
disable_telemetry => true
```

### disable_cleanup
When set to true, storage cleanup won't be done by the plugin (should be done by some other means or by another Logstash process with this flag enabled) Default false
example:
```ruby
disable_cleanup => true
```

### delete_not_notified_blobs
When set to true, not notified blobs are deleted, if not set they are copied to the orphan-blobs container. Default false
example:
```ruby
delete_not_notified_blobs => true
```

### save_notified_blobs_records
When set to true, notified blobs records are saved in the state table, as long as blobs are retained in their containers. Default false
Used for troubleshooting
example:
```ruby
save_notified_blobs_records => true
```

### disable_notification
When set to true, notification is not sent to application insights, but behaves as if notified. Default false
Used for troubleshooting
example:
```ruby
disable_notification => true
```

### disable_blob_upload
When set to true, events are not uploaded, and blob not commited, but behaves as if uploaded and uploaded. Default false
Used for troubleshooting
example:
```ruby
disable_blob_upload => true
```

### stop_on_unknown_io_errors
When set to true, process will stop if an unknown IO error is detected. Default false
Used for troubleshooting
example:
```ruby
stop_on_unknown_io_errors => true
```

### notification_endpoint
when set notification are sent to an alternative endpoint. Default "https://dc.services.visualstudio.com/v2/track"
Used for troubleshooting
example:
```ruby
stop_on_unknown_io_errors => true
```

### notification_version
Advanced, internal, should not be set, the only current valid value is 1

### tables
Allow to support multiple tables, and to configure each table with its own parameters, using the global parameters as defaults.
It is only required if the plugin need to support mutiple table.
Tables is Hash, where the key is the table_id and the value is a has of specific properties, that their defualt value are the global properties.
The specific properties are: intrumentation_key, table_columns, blob_max_delay, csv_default_value, serialized_event_field, blob_serialization, csv_separator
template:
```ruby
tables => { "table_id1" => { properties } "table_id2" => { properties } }
```

Examples:
```ruby
tables => { "6f29a89e-1385-4317-85af-3ac1cea48058" => { "intrumentation_key" => "76c3b8e9-dfc6-4afd-8d4c-3b02fdadb19f", "blob_max_delay" => 60 } }
```

```ruby
tables => { "6f29a89e-1385-4317-85af-3ac1cea48058" => { "intrumentation_key" => "76c3b8e9-dfc6-4afd-8d4c-3b02fdadb19f", "blob_max_delay" => 60 }
            "2e1b46aa-56d2-4e13-a742-d0db516d66fc" => { "intrumentation_key" => "76c3b8e9-dfc6-4afd-8d4c-3b02fdadb19f", "blob_max_delay" => 120 "ext" => "csv" "serialized_event_field" => "message" } 
          }
```

# Configuration example

- Reading data from files and storing in Application Insights:

```ruby
input {
  file {
    path => "/../files/*"
    start_position => "beginning"
  }
}
filter {
  # some filters here
}
output {
  application_insights {
    intrumentation_key => "5a6714a3-ec7b-4999-ab96-232f1da92059"
    table_id => "c24394e1-f077-420e-8a25-ef6fdf045938"
    storage_account_name_key => [ "my-storage-account", "pfrYTwPgKyYNfKBY2QdF+v5sbgx8/eAQp+FFkGpPBnkMDE1k+ZNK3r3qIPqqw8UsOIUqaF3dXBdPDouGJuxNXQ==" ]
  }
}
```

# Enviroment variables
###AZURE_STORAGE_ACCOUNT
Specifies the Azure storage account name
Will be used by the plugin to set the account name part in plugin property **storage_account_name_key** if it is missing
Example:
```sh
AZURE_STORAGE_ACCOUNT="my-storage-account"
```

###AZURE_STORAGE_ACCESS_KEY
Specifies the Azure storage account access key
Will be used by the plugin to set the key part in plugin property **storage_account_name_key** if it is missing
Example:
```sh
AZURE_STORAGE_ACCESS_KEY="pfrYTwPgKyYNfKBY2QdF+v5sbgx8/eAQp+FFkGpPBnkMDE1k+ZNK3r3qIPqqw8UsOIUqaF3dXBdPDouGJuxNXQ=="
```

# Setting up Http/Https Proxy

If you use a proxy server or firewall, you may need to set the HTTP_PROXY and/or HTTPS_PROXY **environment variables** in order to access Azure storage and Application Insights.
Examples:
```sh
HTTP_PROXY=http://proxy.example.org
HTTPS_PROXY=https://proxy.example.org
```

- If the proxy server requires a user name and password, include them in the following form:
```sh
HTTP_PROXY=http://username:password@proxy.example.org
```

- If the proxy server uses a port other than 80, include the port number:
```sh
HTTP_PROXY=http://username:password@proxy.example.org:8080
```

# Setting up SSL certificates

When using SSL/HTTPS, typically log in or authentication may require a CA Authority (CA) certificate. If the required certificate is not already bundled in the system. it may be configured in the plugin (see above ca_file)
example:
```ruby
ca_file => "/path/to/cafile.crt"
```

# Getting Started for Contributors

If you would like to become an active contributor to this project please follow the instructions provided in [Azure Projects Contribution Guidelines](http://azure.github.io/guidelines/).
You can find more details for contributing in the [CONTRIBUTING.md](CONTRIBUTING.md).

# Provide Feedback

If you encounter any bugs with the library please file an issue in the [Issues](https://github.com/Azure/azure-storage-ruby/issues) section of the project.

# Code of Conduct 

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.