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
  class Config

    public

    @@configuration = {}

    def self.current
      @@configuration
    end

    def self.validate_and_adjust_configuration ( configuration )
      configuration.each_pair { |config_name, config_value|
        raise ConfigurationError, "#{config_name.to_s} must be defined" unless config_value || VALID_UNDEFINDED.include?( config_name )
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
            file_name = validate_and_adjust( "#{config_name.to_s}[#{i}]", file_name, String )
            i += 1
            if "stdout" == file_name.downcase
              file_name = :stdout
              file = STDOUT
            elsif "stderr" == file_name.downcase
              file_name = :stderr
              file = STDERR
            else
              file = ::File.open( file_name, "a+" )
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

        when :container_prefix
          container_prefix = validate_and_adjust( config_name, config_value, String )
          unless container_prefix.empty?
            len = 63 - "-#{CONTAINER_LOGSTASH_PREFIX}-yyyy-mm-dd".length
            validate_max( "container_prefix length", container_prefix.length, len )
            container_prefix += "-"
          end
          container_prefix = container_prefix.downcase
          conatiner_name = "#{container_prefix}#{CONTAINER_LOGSTASH_PREFIX}-yyyy-mm-dd"
          raise ConfigurationError, "#{config_name.to_s} must have only alphanumeric and dash characters, cannot start or end with a dash, and a dash cannot follow a dash" unless Utils.valid_container_name?( conatiner_name )
          configuration[config_name] = container_prefix + CONTAINER_LOGSTASH_PREFIX

        when :table_prefix
          table_prefix = validate_and_adjust( config_name, config_value, String )
          unless table_prefix.empty?
            len = 63 - "-#{TABLE_LOGSTASH_PREFIX}yyyymmdd".length
            validate_max( "table_prefix length", table_prefix.length, len )
          end
          table_name = "#{table_prefix}#{TABLE_LOGSTASH_PREFIX}yyyymmdd"
          raise ConfigurationError, "#{config_name} must have only alphanumeric" unless Utils.valid_table_name?( table_name )
          configuration[config_name] = table_prefix + TABLE_LOGSTASH_PREFIX

        when :ca_file
          if config_value
            raise ConfigurationError, "#{config_name} must have a valid path name" unless Utils.valid_file_path?( config_value )
            configuration[config_name] = validate_and_adjust( config_name, config_value, String )
          end

        when :blob_prefix
          blob_prefix = validate_and_adjust( config_name, config_value, String )
          unless blob_prefix.empty?
            len = 1024 - "-#{BLOB_LOGSTASH_PREFIX}_ikey-#{INSTRUMENTATION_KEY_TEMPLATE}_table-#{TABLE_ID_TEMPLATE}_yyyy-mm-dd-HH-MM-SS-LLL".length
            validate_max( "blob_prefix length", blob_prefix.length, len )
            blob_prefix += "-"
          end
          blob_prefix += BLOB_LOGSTASH_PREFIX

          raise ConfigurationError, "#{config_name.to_s} doesn't meet url format" unless Utils.url?( "http://storage/container/#{blob_prefix}_ikey-#{INSTRUMENTATION_KEY_TEMPLATE}_table-#{TABLE_ID_TEMPLATE}.json" )
          configuration[config_name] = blob_prefix

        when :intrumentation_key
          configuration[config_name] = validate_and_adjust_guid( config_name, config_value )

        when :table_id
          configuration[config_name] = validate_and_adjust_guid( config_name, config_value )

        when :event_separator
          configuration[config_name] = validate_and_adjust( config_name, config_value, String )

        when :csv_separator
          configuration[config_name] = validate_and_adjust( config_name, config_value, String )

        when :csv_default_value
          configuration[config_name] = validate_and_adjust( config_name, config_value, String )

        when :blob_max_bytesize
         configuration[config_name] = validate_and_adjust_integer( config_name, config_value, MIN_BLOB_MAX_BYTESIZE, MAX_BLOB_MAX_BYTESIZE )

        when :blob_max_events
         configuration[config_name] = validate_and_adjust_integer( config_name, config_value, MIN_BLOB_MAX_EVENTS, MAX_BLOB_MAX_EVENTS )

        when :blob_max_delay
          configuration[config_name] = validate_and_adjust_number( config_name, config_value, MIN_BLOB_MAX_DELAY, MAX_BLOB_MAX_DELAY )

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
          unless config_value
            raise ConfigurationError, "#{config_name.to_s} is empty, at least one storage account name should be defined" unless ENV['AZURE_STORAGE_ACCOUNT']
            raise ConfigurationError, "#{config_name.to_s} is empty, at least one storage account access key should be defined" unless ENV['AZURE_STORAGE_ACCESS_KEY']
            config_value = [ ENV['AZURE_STORAGE_ACCOUNT'], ENV['AZURE_STORAGE_ACCESS_KEY'] ]
          end
          storage_account_name_key = validate_and_adjust( config_name, config_value, Array, :disallow_empty )
          unless storage_account_name_key[0].is_a?( Array )
            raise ConfigurationError, "#{config_name.to_s} property is empty, should contain at least one pair: account_name, key" unless 2 == storage_account_name_key.length
            storage_account_name_key = [ [ storage_account_name_key[0], storage_account_name_key[1] ]]
          end

          index = 0
          storage_account_name_key.map! { |pair|
            pair = validate_and_adjust( "#{config_name.to_s}[#{index}]", pair, Array, :disallow_empty )
            raise ConfigurationError, "#{config_name.to_s}[#{index}] must have two items" unless 2 == pair.length

            ( name, keys ) = pair
            name = validate_and_adjust( "#{config_name.to_s}[#{index}]:name", name, String, :disallow_empty )
            raise ConfigurationError, "#{config_name.to_s}[#{index}]:name must between 3 to 24 characters" unless (name.length >= 3) && (name.length <= 24)
            raise ConfigurationError, "##{config_name.to_s}[#{index}]:name bad format, must have only alphanumeric characters" unless Utils.alphanumeric?( name )

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

        when :table_ids_properties
          new_table_ids_properties = {}
          table_ids_properties = validate_and_adjust( config_name, config_value, Hash )
          table_ids_properties.each_pair { |table_id, properties|
            table_id = validate_and_adjust_guid( "#{config_name}:table_id", table_id )
            properties = validate_and_adjust( "#{config_name}[#{table_id}]", properties, Hash )

            new_properties = {}

            properties.each_pair { |table_id_property_name, table_id_property_value|
              table_id_property_name = validate_and_adjust( "#{config_name}[#{table_id}]:property", table_id_property_name, String, :disallow_empty )
              info = "#{config_name}[#{table_id}][#{table_id_property_name}]"
              case table_id_property_name.downcase
              when TABLE_ID_PROPERTY_INSTRUMENTATION_KEY
                new_properties[:intrumentation_key] = validate_and_adjust_guid( info, table_id_property_value )
              when TABLE_ID_PROPERTY_EXT
                table_id_property_value = table_id_property_value.downcase
                raise ConfigurationError, "#{info}, can be set to only one of the following values: #{VALID_EXT_EVENT_FORMAT}" unless VALID_EXT_EVENT_FORMAT.include?( table_id_property_value )
                new_properties[:ext] = validate_and_adjust_ext( info ,table_id_property_value, configuration[:blob_prefix] ) # be careful depends on order
              when TABLE_ID_PROPERTY_CSV_DEFAULT_VALUE
                new_properties[:csv_default_value] = validate_and_adjust( info, table_id_property_value, String )
              when TABLE_ID_PROPERTY_CSV_SEPARATOR
                new_properties[:csv_separator] = validate_and_adjust( info, table_id_property_value, String )
              when TABLE_ID_PROPERTY_MAX_DELAY
                new_properties[:blob_max_delay] = validate_and_adjust_number( info, table_id_property_value, MIN_BLOB_MAX_DELAY, MAX_BLOB_MAX_DELAY )
              when TABLE_ID_PROPERTY_EVENT_SEPARATOR
                new_properties[:event_separator] = validate_and_adjust( info, table_id_property_value, String )
              when TABLE_ID_PROPERTY_DATA_FIELD
                new_properties[:data_field] = validate_and_adjust( info, table_id_property_value, String )
              when TABLE_ID_PROPERTY_FIELDS_MAP
                fields_map = table_id_property_value
                fields_map = [ fields_map ] if fields_map.is_a?( String )
                fields_map = validate_and_adjust( info, fields_map, Array, :disallow_empty )
                new_fields_map = []
                index = 0
                fields_map.each do |column|
                  new_column = {}

                  column = { TABLE_ID_PROPERTY_FIELDS_MAP_NAME => column } if column.is_a?( String )
                  column = validate_and_adjust( "#{info}[#{index}]", column, Hash, :disallow_empty )
                  raise ConfigurationError, "#{info}[#{index}][#{TABLE_ID_PROPERTY_FIELDS_MAP_NAME}] must be defined" unless column[TABLE_ID_PROPERTY_FIELDS_MAP_NAME]
                  new_column[:name] = validate_and_adjust( "#{info}[#{index}][#{TABLE_ID_PROPERTY_FIELDS_MAP_NAME}]", column[TABLE_ID_PROPERTY_FIELDS_MAP_NAME], String )

                  if column[TABLE_ID_PROPERTY_FIELDS_MAP_DEFAULT]
                    new_column[:default] = validate_and_adjust( "#{info}[#{index}][#{TABLE_ID_PROPERTY_FIELDS_MAP_DEFAULT}]", column[TABLE_ID_PROPERTY_FIELDS_MAP_DEFAULT], String )
                  end

                  if column[TABLE_ID_PROPERTY_FIELDS_MAP_TYPE]
                    new_column[:type] = validate_and_adjust( "#{info}[#{index}][#{TABLE_ID_PROPERTY_FIELDS_MAP_TYPE}]", column[TABLE_ID_PROPERTY_FIELDS_MAP_TYPE], String ).downcase
                    raise ConfigurationError, "#{info}[#{index}][#{TABLE_ID_PROPERTY_FIELDS_MAP_TYPE}] can be only one of the following values: #{VALID_FIELDS_MAP_TYPES}" unless VALID_FIELDS_MAP_TYPES.any? {|type| type == new_column[:type]}
                    new_column[:type] = new_column[:type].to_sym
                  end
                  new_fields_map << new_column
                  index += 1
                end
                new_properties[:fields_map] = new_fields_map
              else
              end
            }
            if new_properties[:ext] && EXT_EVENT_FORMAT_CSV == new_properties[:ext] && new_properties[:fields_map].nil? &&  new_properties[:data_field].nil?
              raise ConfigurationError, "#{config_name}[#{table_id}] cannot be set to csv extension, without setting fields_map or data_field"
            end
            new_table_ids_properties[table_id] = new_properties
          }
          configuration[config_name] = new_table_ids_properties
        else
          configuration[config_name] = config_value
        end
      }
      @@configuration = configuration
    end



    private 

    def self.validate_and_adjust_ext ( property, ext, prefix )
      ext = validate_and_adjust( property, ext, String )
      raise ConfigurationError, "#{property.to_s} must be a valid extension string, have only alphanumeric, dash and underline characters" unless Utils.ext?( ext )
      len = 1024 - "#{prefix}-#{BLOB_LOGSTASH_PREFIX}_ikey-#{INSTRUMENTATION_KEY_TEMPLATE}_table-#{TABLE_ID_TEMPLATE}_yyyy-mm-dd-HH-MM-SS-LLL".length
      raise ConfigurationError, "#{property.to_s} length cannot be more than #{len} characters" unless ext.length <= len
      ext
    end

    def self.validate_and_adjust ( property, value, type, disallow_empty = false )
      raise ConfigurationError, "#{property.to_s} must be an #{type.to_s}" unless value.is_a?( type )
      raise ConfigurationError, "#{property.to_s} cannot be empty" if disallow_empty && value.empty? 
      return value unless String == type
      Utils.unescape( value )
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
