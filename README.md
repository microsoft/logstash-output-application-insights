# Microsoft Application Insights Output Plugin

This is a plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.


[![Gem Version](https://badge.fury.io/rb/azure-storage.svg)](https://badge.fury.io/rb/azure-storage)
* Master: [![Master Build Status](https://travis-ci.org/Azure/azure-storage-ruby.svg?branch=master)](https://travis-ci.org/Azure/azure-storage-ruby/branches) [![Coverage Status](https://coveralls.io/repos/github/Azure/azure-storage-ruby/badge.svg?branch=master)](https://coveralls.io/github/Azure/azure-storage-ruby?branch=master)
* Dev: [![Dev Build Status](https://travis-ci.org/Azure/azure-storage-ruby.svg?branch=dev)](https://travis-ci.org/Azure/azure-storage-ruby/branches) [![Coverage Status](https://coveralls.io/repos/github/Azure/azure-storage-ruby/badge.svg?branch=dev)](https://coveralls.io/github/Azure/azure-storage-ruby?branch=dev)

This project provides a Ruby package that makes it easy to access and manage Microsoft Azure Storage Services.

# Plugin Features

# Supported Logstash Versions

* Logstash 2.3.2
* Logstash 2.3.4

Note: 

* x64 Ruby for Windows is known to have some compatibility issues.
* the plugin depends on azure-storage that depends on gem nokogiri, which doesn't support Ruby 2.2+ on Windows.

### Running Plugin in Logstash

#### Run in a local Logstash clone

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
#### Run in an installed Logstash

You can use the same method as in local Logstash clone to run the plugin in an installed Logstash by editing its `Gemfile` and pointing the `:path` to the local plugin directory or you can build the gem and install it using:

- Build your plugin gem
```sh
gem build logstash-output-application-insights.gemspec
```
- Install the plugin from the Logstash home
```sh
# Logstash 2.3 and higher
bin/logstash-plugin install --no-verify

```
- Start Logstash and use the plugin

## Plugin configuration parameters

### storage_account_name_ke
Array of pairs, storage_account_name and an array of acces_keys 
examples [ account1, key1 ]
         [ [account1, key1], [accoun2, key2], [account3, key3] ]
         [ [account1, [key11, key12]], [account2, key2], [account3, [key3] ]

### azure_storage_table_prefix
prefix for the azure storage tables used by this Logstash instance
it is recommeded that each Logstash prefix have a unique prefix, to avoid
confusion and loss of tracking, although sharing tables won't damage proper execution
if not set, the machine name is used (not alphanumeric characters are removed, and converted downcase), if machine name available
string may contain only alphanumeric character, case sensitive, and must start with a letter 

### azure_storage_container_prefix
prefix for the azure storage containers used by this Logstash instance
it is recommeded that each Logstash prefix have a unique prefix, to avoid
confusion and loss of tracking, although sharing containers won't damage proper execution
if not set, the machine name is used (not alphanumeric characters are removed, and converted downcase), if machine name available
string may contain only alphanumeric character and dash, double dash not allowed, case insesitive

### azure_storage_blob_prefix
prefix for the azure storage blobs used by this Logstash instance
Each Logstash prefix MUST have a unique prefix, to avoid
loss of data !!!
if not set, the machine name is used (not alphanumeric characters are removed, and converted downcase), if machine name available
string may include only characters that are allowed in any valid url

### intrumentation_key
Default Application Insights Analytics intrumentation_key
will be used only in case it is not specified as a table_id property in tables
or as part of the event's fields or event's metadata fields

### table_id
Default Application Insights Analytics table_id
will be used only in case it is not specified as part o
of the event's fields or event's metadata fields

### table_columns
specifies the list of the fields that will be filtered from the events, fields not specified will be ignored.
if not specified all fileds in events will be filtered, the order is kept, and is used for csv serialization

### case_insensitive_columns
if set to true events fields are refered as case insensitive, default case sensitive 

### tables
A hash of table_ids, where each table_id points to a set of properties
the properties are a hash, where the keys are are the properties
current supported properties per table_id are:
intrumentation_key, ext, table_columns, csv_default_value, csv_separator, max_delay, event_separator, data_field
intrumentation_key, Application Insights Analytics intrumentation_key, will be used in case not specified in any of the event's fields or events's metadata fileds
data_field, specifies the data field that may contain the full serialized event (either as json or csv), 
            when specified, the ext property should be set either to csv or to json (json is the default)
            if event. does not conatin the field, value will be created based on the fileds in the evnt, according to table_columns if configured, or all fileds in event
            if event contains this filed, and ext is csv
                 if value is a string, it will be used as is as the serialized event, without validating whether it is a csv string
                 if value is an array, it will be serialized as an array of csv columns
                 if value is a hash, it will be serialized based on table_columns to csv columns
            if event contains this filed, and ext is json
                 if value is a string, it will be used as is as the serialized event, without validating whether it is a json string
                 if value is a hash, it will be serialized to json, if fileds_map exit, it will be based on filds_map
                 if value is an array, it will be zipped with table_columns (if exist) and serialized to json
ext, blob extension, the only valid values are either csv or json, 
            should be set whenever the default json is not appropriate (.e, csv)
max_delay, maximum latency time, in seconds, since the time the event arrived till it should be commited in azure storage, and Application Insights is notified
event_separator, specifies the string that is used as a separator between events in the blob
table_columns, specifies the event fields that should be serialized, and their order (order is required for csv)
         if csv serialization will be used for this table_id
         each table_columns field is a hash with 3 keys: name, type, and default. Only name is mandatory
             name - is the name of the event fleld that its value should be mapped to this columns
             type - is the type of this field: "string", "hash", "array", "number", "json", "boolean", "float", "integer", "dynamic", "datetime", "object"
             default - is the value to be used for this column, in case the field is missing in the event
csv_separator, specifies the string that is used as a separator between columns, 
             can be specified only together with table_columns
csv_default_value, specifies the string that is used as the value in a csv record, in case the field does not exist in the event
             can be specified only together with table_columns
Example json table_id
  tables => {"a679fbd2-702c-4c46-8548-80082c66ef28" => {"intrumentation_key" => "abee940b-e648-4242-b6b3-f2826667bf96", "max_delay" => 60} }
Example json table_id, input in data_field
  {"ab6a3584-aef0-4a82-8725-2f2336e59f3e" => {"data_field" => "message". "ext" => "json"} }
Example csv table_id, input in data_field
  {"ab6a3584-aef0-4a82-8725-2f2336e59f3e" => {"data_field" => "csv_message". "ext" => "csv"} }
Example csv table_id, input in event fields
  {"ab6a3584-aef0-4a82-8725-2f2336e59f3e" => { "ext" => "csv", "table_columns" => [ {name => "Timestamp" type => datetime }, "Value", "Custom" ] } }
Example csv table_id, input in event fields
  {"ab6a3584-aef0-4a82-8725-2f2336e59f3e" => { "ext" => "json", "table_columns" => [ "Timestamp", "Value", "Custom" ] } }

### blob_max_bytesize
Advanced, internal, should not be set, the default is 192 GB ( = 50,000 * 4 MB ) 
azure storage maximum number of blocks per blob is 192 GB ( = 50,000 * 4 MB ) 

### blob_max_events
Specifies, maximum number of events in one blob,
setting it low may reduce the latency, but reduce logstash performance
setting it high may increase latency to maximum delay, but logstash will be more efficient, and load on network will be lower

### blob_max_delay
Specifies maximum latency time, in seconds, since the time the event arrived 
till it is commited to azure storage, and Application Insights is notified
The total latency may be higher, as this is not the full ingestion flow 

### io_retry_delay
Interval of time between retries due to IO failures

### io_max_retries
Number of retries on IO failures, before giving up, and move to available options

### blob_retention_time
Specifies the retention time of the blob in the container after it is notified to Application Insighta Analytics
Once the retention time expires, the blob is the deleted from container

### blob_access_expiry_time
Specifies the time Application Insights Analytics have access to the blob that are notifie
Blob access is limited with SAS URL

### event_separator
Advanced, internal, should not be set, the default is \r\n, 
specifies the string that is used as a separator between events in the blob

### csv_separator
Advanced, internal, should not be set, the default is comma, 
specifies the string that is used as a separator between columns in a csv record

### csv_default_value
specifies the string that is used as the value in a csv record, in case the field does not exist in the event, the default is ""

### logger_level
specifies the log level. valid values are: DEBUG, INFO, WARN, ERROR, FATAL, UNKNOWN

### logger_files
Specifies the list of targets for the log. may include files, devices, "stdout: and "stderr"

### logger_shift_progname
Advanced, internal, should not be set, the default is AI, 
Specifies the program name that will displayed in each log record

### logger_shift_size
Specifies when file logs are shifted. valid values are either an integer or "daily", "weekly" or "monthly"

### logger_shift_age
Specifies the shift age of a log

### resurrect_delay
Specifies the time interval, between tests that check whether a stoarge account came back to life, 
after it stoped responding 

### flow_control_suspend_bytes
specifies the high water mark for the flow control, that used to avoid out of memory crash
once memory consumption reach reach the high water mark, the plugin will stop accepting events, till memory
is below the low water mark

### flow_control_resume_bytes
specifies the low water mark for the flow control, that used to avoid out of memory crash
once memory consumption reach the high water mark, the plugin will stop accepting events, till memory
is below the low water mark

### flow_control_delay
specifies the amount of time the flow control suspend receiving event, to allow GC, and flush of event to Azure storage
before checking whether memory is below low water mark

### ca_file
File path of the CA file if having issue with SSL

### disable_telemetry
When set to true, telemetry about the plugin, won't be sent to Application Insights

### disable_cleanup
When set to true, storage cleanup won't be done by the plugin (should be done by some other means or by another Logstash process with this flag enabled)

### delete_not_notified_blobs
When set to true, not notified blobs are deleted, if not set they are copied to the orphan-blobs container

### save_notified_blobs_records
When set to true, notified blobs records are save in table, as long as blobs are retained in their containers

### disable_notification
Advanced, internal, should not be set, the default is false
When set to true, notification is not sent to application insights, but behaves as if notified

### disable_blob_upload
Advanced, internal, should not be set, the default is false
When set to true, events are not uploaded, and blob not commited, but behaves as if uploaded and notified

### stop_on_unknown_io_errors
Advanced, internal, should not be set, the default is false
When set to true, process will stop if an unknown IO error is found

### notification_endpoint
Advanced, internal, should not be set, the default is Application Insights production endpoint
when set notification are sent to an alternative endpoint, used for internal testing

### notification_version
Advanced, internal, should not be set, the only current valid value is 1


## Example configuration

- Reading data from files and storing in Application Insights:

input {
  file {
    path => "/../files/*"
    start_position => "beginning"
  }
}
	filter {
	  [some filters here]
	}
	output {
	  application_insights {
	    intrumentation_key => "5A6714A3-EC7B-4999-AB96-232F1DA92059"
      table_id => "C24394E1-F077-420E-8A25-EF6FDF045938"
      storage_account_name_key => [ "my-storage-account", "pfrYTwPgKyYNfKBY2QdF+v5sbgx8/eAQp+FFkGpPBnkMDE1k+ZNK3r3qIPqqw8UsOIUqaF3dXBdPDouGJuxNXQ==" ]
	  }
	}


# Getting Started for Contributors

If you would like to become an active contributor to this project please follow the instructions provided in [Azure Projects Contribution Guidelines](http://azure.github.io/guidelines/).
You can find more details for contributing in the [CONTRIBUTING.md](CONTRIBUTING.md).

# Provide Feedback

If you encounter any bugs with the library please file an issue in the [Issues](https://github.com/Azure/azure-storage-ruby/issues) section of the project.

# Code of Conduct 

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.