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
  class Config

    public

    @@configuration = {}
    @@masked_configuration = {}

    def self.current
      @@configuration
    end

    def self.masked_current
      @@masked_configuration
    end

    def self.validate_and_adjust_configuration ( configuration )
      configuration.each_pair { |config_name, config_value|
        raise ConfigurationError, "#{config_name.to_s} must be defined" unless config_value || BOOLEAN_PROPERTIES.include?( config_name )
        case config_name

        when :logger_level
          logger_level = validate_and_adjust( config_name, config_value, String )
          raise ConfigurationError, "#{config_name.to_s} can be set to only one of the valid log levels" unless LOGGER_LEVEL_MAP[logger_level.upcase.to_sym]
          configuration[config_name] = LOGGER_LEVEL_MAP[logger_level.upcase.to_sym]

        when :logger_files
          config_value = [ config_value ] if config_value.is_a?( String )
          logger_files = validate_and_adjust( config_name, config_value, Array )

          i = 0
          logger_files.map! do |file_name|
            file_name = validate_and_adjust( "#{config_name}[#{i}]", file_name, String )
            i += 1
            if "stdout" == file_name.downcase
              file_name = :stdout
              file = STDOUT
            elsif "stderr" == file_name.downcase
              file_name = :stderr
              file = STDERR
            else
              begin
                file = ::File.open( file_name, "a+" )
              rescue => e
                raise ConfigurationError, "#{config_name}[#{i}] cannot open file #{file_name}, due to error #{e.inspect}"
              end
            end
            [ file_name, file ]
          end
          configuration[config_name] = logger_files

        when :logger_progname
          configuration[config_name] = validate_and_adjust( config_name, config_value, String )

        when :logger_shift_size
          configuration[config_name] = validate_and_adjust_integer( config_name, config_value, MIN_LOGGER_SHIFT_SIZE, MAX_LOGGER_SHIFT_SIZE )

        when :logger_shift_age
          if config_value.is_a?( String )
            config_value = validate_and_adjust( config_name, config_value, String ).downcase
            raise ConfigurationError, "#{config_name.to_s} if string, can be set to only one of the following values: #{VALID_LOGGER_SHIFT_AGES}" unless VALID_LOGGER_SHIFT_AGES. include?( config_value )
          elsif config_value.is_a?( Integer )
            config_value = validate_and_adjust_integer( config_name, config_value, MIN_LOGGER_SHIFT_AGE, MAX_LOGGER_SHIFT_AGE )
          else
            raise ConfigurationError, "#{config_name.to_s} must be either a string or integer"
          end
          configuration[config_name] = config_value

        when :azure_storage_container_prefix
          unless config_value.empty?
            azure_storage_container_prefix = validate_and_adjust( config_name, config_value, String )
            len = 63 - "#{AZURE_STORAGE_CONTAINER_LOGSTASH_PREFIX}--yyyy-mm-dd".length
            validate_max( "azure_storage_container_prefix length", azure_storage_container_prefix.length, len )
            azure_storage_container_prefix = azure_storage_container_prefix.downcase
            container_name = "#{AZURE_STORAGE_CONTAINER_LOGSTASH_PREFIX}-#{azure_storage_container_prefix}-yyyy-mm-dd"
            raise ConfigurationError, "#{config_name.to_s} must have only alphanumeric characters and dash, cannot start or end with a dash, and a dash cannot follow a dash" unless Utils.valid_container_name?( container_name )
            configuration[config_name] = "-#{azure_storage_container_prefix}"
          end

        when :azure_storage_table_prefix
          unless config_value.empty?
            azure_storage_table_prefix = validate_and_adjust( config_name, config_value, String )
            len = 63 - "#{AZURE_STORAGE_TABLE_LOGSTASH_PREFIX}yyyymmdd".length
            validate_max( "azure_storage_table_prefix length", azure_storage_table_prefix.length, len )
            table_name = "#{AZURE_STORAGE_TABLE_LOGSTASH_PREFIX}#{azure_storage_table_prefix}yyyymmdd"
            raise ConfigurationError, "#{config_name} must have only alphanumeric" unless Utils.valid_table_name?( table_name )
            configuration[config_name] = azure_storage_table_prefix
          end

        when :ca_file
          config_value = validate_and_adjust( config_name, config_value, String )
          unless config_value.empty?
            raise ConfigurationError, "#{config_name} must have a valid path name" unless Utils.valid_file_path?( config_value )
          end
          configuration[config_name] = validate_and_adjust( config_name, config_value, String )

        when :azure_storage_host_suffix
          config_value = validate_and_adjust( config_name, config_value, String )
          unless config_value.empty?
            raise ConfigurationError, "#{config_name} must have a valid host DNS address" unless Utils.dns_address?( config_value )
          end
          configuration[config_name] = validate_and_adjust( config_name, config_value, String )

        when :azure_storage_blob_prefix
          unless config_value.empty?
            azure_storage_blob_prefix = validate_and_adjust( config_name, config_value, String )
            len = 1024 - "#{AZURE_STORAGE_BLOB_LOGSTASH_PREFIX}//ikey-#{INSTRUMENTATION_KEY_TEMPLATE}/table-#{TABLE_ID_TEMPLATE}/yyyy-mm-dd-HH-MM-SS-LLL_0000.json".length
            validate_max( "azure_storage_blob_prefix length", azure_storage_blob_prefix.length, len )
            raise ConfigurationError, "#{config_name.to_s} doesn't meet url format" unless Utils.url?( "http://storage/container/#{azure_storage_blob_prefix}_ikey-#{INSTRUMENTATION_KEY_TEMPLATE}_table-#{TABLE_ID_TEMPLATE}.json" )
            configuration[config_name] = "/#{azure_storage_blob_prefix}"
          end

        when :table_id
          configuration[config_name] = validate_and_adjust_guid( config_name, config_value )

        when :blob_max_bytesize
         configuration[config_name] = validate_and_adjust_integer( config_name, config_value, MIN_BLOB_MAX_BYTESIZE, MAX_BLOB_MAX_BYTESIZE )

        when :blob_max_events
         configuration[config_name] = validate_and_adjust_integer( config_name, config_value, MIN_BLOB_MAX_EVENTS, MAX_BLOB_MAX_EVENTS )

        when :blob_retention_time
          configuration[config_name] = validate_and_adjust_number( config_name, config_value, MIN_BLOB_RETENTION_TIME, MAX_BLOB_RETENTION_TIME )

        when :blob_access_expiry_time
          configuration[config_name] = validate_and_adjust_number( config_name, config_value, MIN_BLOB_ACCESS_EXPIRY_TIME, MAX_BLOB_ACCESS_EXPIRY_TIME )

        when :resurrect_delay
          configuration[config_name] = validate_and_adjust_number( config_name, config_value, MIN_STORAGE_RESURRECT_DELAY, MAX_STORAGE_RESURRECT_DELAY )

        when :flow_control_suspend_bytes
          configuration[config_name] = validate_and_adjust_number( config_name, config_value, MIN_FLOW_CONTROL_SUSPEND_BYTES, MAX_FLOW_CONTROL_SUSPEND_BYTES )

        when :flow_control_resume_bytes
          configuration[config_name] = validate_and_adjust_number( config_name, config_value, MIN_FLOW_CONTROL_RESUME_BYTES, MAX_FLOW_CONTROL_RESUME_BYTES )

        when :flow_control_delay
          configuration[config_name] = validate_and_adjust_number( config_name, config_value, MIN_FLOW_CONTROL_DELAY, MAX_FLOW_CONTROL_DELAY )

        when :io_retry_delay
          configuration[config_name] = validate_and_adjust_number( config_name, config_value, MIN_IO_RETRY_DELAY, MAX_IO_RETRY_DELAY )

        when :io_max_retries
          configuration[config_name] = validate_and_adjust_integer( config_name, config_value, MIN_IO_MAX_RETRIES, MAX_IO_MAX_RETRIES )

        when :storage_account_name_key
          config_value = validate_and_adjust( config_name, config_value, Array )
          if config_value.empty?
            raise ConfigurationError, "#{config_name} is empty, at least one storage account name should be defined" unless ENV['AZURE_STORAGE_ACCOUNT']
            raise ConfigurationError, "#{config_name} is empty, at least one storage account access key should be defined" unless ENV['AZURE_STORAGE_ACCESS_KEY']
            config_value = [ ENV['AZURE_STORAGE_ACCOUNT'], ENV['AZURE_STORAGE_ACCESS_KEY'] ]
          end

          storage_account_name_key = validate_and_adjust( config_name, config_value, Array, :disallow_empty )
          unless storage_account_name_key[0].is_a?( Array )
            raise ConfigurationError, "#{config_name} property is empty, should contain at least one pair: account_name, key" unless 2 == storage_account_name_key.length
            storage_account_name_key = [ [ storage_account_name_key[0], storage_account_name_key[1] ]]
          end

          index = 0
          storage_account_name_key.map! { |pair|
            pair = validate_and_adjust( "#{config_name}[#{index}]", pair, Array, :disallow_empty )
            raise ConfigurationError, "#{config_name}[#{index}] must have two items" unless 2 == pair.length

            ( name, keys ) = pair
            name = validate_and_adjust( "#{config_name}[#{index}]:name", name, String, :disallow_empty )
            raise ConfigurationError, "#{config_name}[#{index}]:name must between 3 to 24 characters" unless (name.length >= 3) && (name.length <= 24)
            raise ConfigurationError, "##{config_name}[#{index}]:name bad format, must have only alphanumeric characters" unless Utils.alphanumeric?( name )

            keys = [ keys ] if keys.is_a?( String )
            keys = validate_and_adjust( "#{config_name}[#{index}]:keys", keys, Array, :disallow_empty )
            keys.each_index do |i|
              key = validate_and_adjust( "#{config_name}[#{index}:keys[#{i}]", keys[i], String, :disallow_empty )
              raise ConfigurationError, "#{config_name}[#{index}:keys[#{i}] must have only valid base64 characters" unless Utils.base64?( key )
            end
            index += 1
            [ name.downcase, keys ]
          }
          configuration[config_name] = storage_account_name_key

        when :tables
          tables = validate_and_adjust( config_name, config_value, Hash )
          tables.each_pair { |table_id, properties|
            table_id = validate_and_adjust_guid( "#{config_name}:table_id", table_id )
            info = "#{config_name}[#{table_id}]"
            properties = validate_and_adjust( info, properties, Hash )
            properties = Utils.symbolize_hash_keys( properties )
            validate_and_adjust_table_properties!( properties, configuration, info )
            configuration[config_name][table_id] = properties
          }
        end
      }
      validate_and_adjust_table_properties!( configuration, configuration )

      configuration[:state_table_name] = "#{AZURE_STORAGE_TABLE_LOGSTASH_PREFIX}#{configuration[:azure_storage_table_prefix]}#{STATE_TABLE_NAME}"
      configuration[:test_storage_container] = "#{AZURE_STORAGE_CONTAINER_LOGSTASH_PREFIX}#{configuration[:azure_storage_container_prefix]}-#{STORAGE_TEST_CONTAINER_NAME}"
      configuration[:partition_key_prefix] = configuration[:azure_storage_blob_prefix].gsub( "/", "" )

      @@masked_configuration = mask_configuration( configuration )

      @@configuration = configuration
    end

    def self.mask_configuration ( configuration )
      masked_configuration = configuration.dup
      storage_account_name_key = masked_configuration[:storage_account_name_key]
      masked_storage_account_name_key = storage_account_name_key.map { |pair|
        ( name, keys ) = pair
        masked_keys = keys.map { |key| "*****************" }
        [ name, masked_keys ]
      }
      masked_configuration[:storage_account_name_key] = masked_storage_account_name_key
      masked_configuration
    end

    def self.symbolize_table_properties ( properties )
      new_properties = {}
      properties.each_pair { |property_name, property_value|
        new_properties[property_name.to_sym] = property_value
      }
      new_properties
    end

    def self.validate_and_adjust_table_properties! ( properties, configuration, base_info = nil )

      properties.each_pair { |property_name, property_value|
        info = ( base_info.nil? ? "#{property_name}" : "#{base_info}[#{property_name}]" )
        raise ConfigurationError, "#{info} must be defined" unless property_value || BOOLEAN_PROPERTIES.include?( property_name )

        case property_name.downcase

        when :instrumentation_key
          properties[:instrumentation_key] = validate_and_adjust_guid( info, property_value )
        when :blob_serialization
          property_value = property_value.downcase
          raise ConfigurationError, "#{info}, can be set to only one of the following values: #{VALID_EXT_EVENT_FORMAT}" unless VALID_EXT_EVENT_FORMAT.include?( property_value )
          properties[:blob_serialization] = validate_and_adjust_ext( info ,property_value, configuration[:azure_storage_blob_prefix] ) # be careful depends on order
        when :csv_default_value
          properties[:csv_default_value] = validate_and_adjust( info, property_value, String )
        when :csv_separator
          properties[:csv_separator] = validate_and_adjust( info, property_value, String )
        when :blob_max_delay
          properties[:blob_max_delay] = validate_and_adjust_number( info, property_value, MIN_BLOB_MAX_DELAY, MAX_BLOB_MAX_DELAY )
        when :event_separator
          properties[:event_separator] = validate_and_adjust( info, property_value, String )
        when :serialized_event_field
          properties[:serialized_event_field] = ( property_value.nil? ? nil : validate_and_adjust( info, property_value, String ) )
        when :case_insensitive_columns
          properties[:case_insensitive_columns] = validate_and_adjust_boolean( info, property_value )
        when :table_columns
          if property_value.nil?
            properties[:table_columns] = property_value
          else
            table_columns = property_value
            table_columns = [ table_columns ] if table_columns.is_a?( String )
            table_columns = validate_and_adjust( info, table_columns, Array)
            new_table_columns = []
            index = 0
            table_columns.each do |column|
              new_column = {}

              column = { COLUMN_PROPERTY_NAME => column } if column.is_a?( String )
              column = validate_and_adjust( "#{info}[#{index}]", column, Hash, :disallow_empty )
              raise ConfigurationError, "#{info}[#{index}][#{COLUMN_PROPERTY_NAME}] must be defined" unless column[COLUMN_PROPERTY_NAME]
              new_column[:name] = validate_and_adjust( "#{info}[#{index}][#{COLUMN_PROPERTY_NAME}]", column[COLUMN_PROPERTY_NAME], String )

              if column[COLUMN_PROPERTY_DEFAULT]
                new_column[:default] = validate_and_adjust( "#{info}[#{index}][#{COLUMN_PROPERTY_DEFAULT}]", column[COLUMN_PROPERTY_DEFAULT], String )
              end

              if column[COLUMN_PROPERTY_TYPE]
                new_column[:type] = validate_and_adjust( "#{info}[#{index}][#{COLUMN_PROPERTY_TYPE}]", column[COLUMN_PROPERTY_TYPE], String ).downcase
                raise ConfigurationError, "#{info}[#{index}][#{COLUMN_PROPERTY_TYPE}] can be only one of the following values: #{VALID_FIELDS_MAP_TYPES}" unless VALID_FIELDS_MAP_TYPES.any? {|type| type == new_column[:type]}
                new_column[:type] = new_column[:type].to_sym
              end
              new_table_columns << new_column
              index += 1
            end
            properties[:table_columns] = new_table_columns
          end

        else
        end
      }
    end


    private 

    def self.validate_and_adjust_ext ( property, ext, prefix )
      ext = validate_and_adjust( property, ext, String )
      raise ConfigurationError, "#{property.to_s} must be a valid extension string, have only alphanumeric, dash and underline characters" unless Utils.ext?( ext )
      len = 1024 - "#{AZURE_STORAGE_BLOB_LOGSTASH_PREFIX}#{prefix}/ikey-#{INSTRUMENTATION_KEY_TEMPLATE}/table-#{TABLE_ID_TEMPLATE}/yyyy-mm-dd-HH-MM-SS-LLL_0000".length
      raise ConfigurationError, "#{property.to_s} length cannot be more than #{len} characters" unless ext.length <= len
      ext
    end

    def self.validate_and_adjust ( property, value, type, disallow_empty = false )
      raise ConfigurationError, "#{property.to_s} must be an #{type.to_s}" unless value.is_a?( type )
      raise ConfigurationError, "#{property.to_s} cannot be empty" if disallow_empty && value.empty? 
      return value unless String == type
      Utils.unescape( value )
    end

    def self.validate_and_adjust_boolean( property, value )
      raise ConfigurationError, "#{property.to_s} must be a Boolean" unless !!value == value
      value
    end


    def self.validate_and_adjust_integer ( property, value, min, max )
      if value.is_a?( String )
        raise ConfigurationError, "#{property.to_s} must be an Integer or Integer string" unless Utils.integer?( value )
        value = value.to_i
      end
      raise ConfigurationError, "#{property.to_s} must be an Integer" unless value.is_a?( Integer )
      validate_max( property, value, max )
      validate_min( property, value, min )
      value
    end

    def self.validate_and_adjust_number ( property, value, min, max )
      if value.is_a?( String )
        raise ConfigurationError, "#{property.to_s} must be a number or number string" unless Utils.number?( value )
        value = value.to_f
      end
      raise ConfigurationError, "#{property.to_s} must be an Integer" unless value.is_a?( Integer )
      validate_max( property, value, max )
      validate_min( property, value, min )
      value
    end

    def self.validate_and_adjust_guid ( property, value )
      value = validate_and_adjust( property, value, String )
      raise ConfigurationError, "#{property.to_s} must be a Guid string, that followy this pattern xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" unless Utils.guid?( value )
      value
    end

    def self.validate_max (property, value, max)
      raise ConfigurationError, "#{property.to_s} cannot be bigger than #{max}" unless value <= max || 0 == max
      value
    end

    def self.validate_min (property, value, min)
      raise ConfigurationError, "#{property.to_s} cannot be less than #{min}" unless value >= min
      value
    end

  end
end
