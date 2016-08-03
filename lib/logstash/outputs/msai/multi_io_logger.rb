# encoding: utf-8

class LogStash::Outputs::Msai
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
          puts "logger do not support log shifting on this OS"
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


