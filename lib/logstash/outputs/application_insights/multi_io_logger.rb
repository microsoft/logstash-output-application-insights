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

class LogStash::Outputs::Application_insights
  class Multi_io_logger

    def self.config ( configuration )
      files = configuration[:logger_files].dup
      level = configuration[:logger_level]
      progname = configuration[:logger_progname]
      shift_size = configuration[:logger_shift_size]
      shift_age = configuration[:logger_shift_age]

      files.rotate! if files.length > 1 && [ :stdout, :stderr ].include?( files[0][0] )
      (name, file) = files.shift || [ :stdout, STDOUT ]
      if [ :stdout, :stderr ].include?( name )
        logger = Logger.new( file )
      else
        begin
          file_renamed = File.rename( name, "#{name}.20160714" )
          file_renamed = File.rename( "#{name}.20160714", name )
          logger = Logger.new( name, shift_age, shift_size )
        rescue
          puts "--- logger do not support log shifting on this OS"
          logger = Logger.new( name )
        end

      end
      unless files.empty?
        @@targets = files
        logger.formatter = proc do |severity, datetime, progname, msg|
          formatted_msg = "#{severity[0]}, [#{datetime.utc.iso8601(6)} ##{Process.pid}]  #{severity} -- #{progname}: #{msg}#{$/}"
          begin
            @@targets.each do |name, t| 
              t.write( formatted_msg )
              t.flush 
            end
          rescue # ignore errors
          end
          formatted_msg
        end
      end
      logger.progname = progname
      logger.level = ( level unless configuration[:logger_files].empty? ) || Logger::UNKNOWN
      configuration[:logger] = logger
    end

  end
end


