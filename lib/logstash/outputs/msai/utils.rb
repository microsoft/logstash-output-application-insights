# encoding: utf-8

class LogStash::Outputs::Msai
  class Utils

    def self.string_to_hex_string(str, readable = true) 
      unpacked = str.unpack('H*').first 
      if readable 
        unpacked.gsub(/(..)/,'\1 ').rstrip 
      else 
        unpacked 
      end 
 	  end 
    UNESCAPES = {
        'a' => "\x07", 'b' => "\x08", 't' => "\x09",
        'n' => "\x0a", 'v' => "\x0b", 'f' => "\x0c",
        'r' => "\x0d", 'e' => "\x1b", "\\\\" => "\x5c",
        "\"" => "\x22", "'" => "\x27"
    }

    def self.os
      host_os = RbConfig::CONFIG['host_os']
      case host_os
      when /mswin|msys|mingw|cygwin|bccwin|wince|emc/
        "Windows #{host_os}"
      when /darwin|mac os/
        "MacOS #{host_os}"
      when /linux/
        "Linux #{host_os}"
      when /solaris|bsd/
        "Unix #{host_os}"
      else
        "Unknown #{host_os}"
      end
    end


    def self.unescape(str)
      # Escape all the things
      str.gsub(/\\(?:([#{UNESCAPES.keys.join}])|u([\da-fA-F]{4}))|\\0?x([\da-fA-F]{2})/) {
        if $1
          if $1 == '\\' then '\\' else UNESCAPES[$1] end
        elsif $2 # escape \u0000 unicode
          ["#$2".hex].pack('U*')
        elsif $3 # escape \0xff or \xff
          [$3].pack('H2')
        end
      }
    end

    def self.integer? ( s )
      s =~ /\A[-+]?[0-9]*\z/
    end
    
    def self.numeric? ( s )
      s =~ /\A[-+]?[0-9]*\.?[0-9]+\z/
    end

    def self.guid? ( s )
      s =~ /\A[\da-f]{8}-([\da-f]{4}-){3}[\da-f]{12}\z/i
    end

    def self.valid_container_name? ( s )
      s =~ /\A[a-z0-9](?:[a-z0-9]|(\-(?!\-))){1,61}[a-z0-9]\z/
    end

    def self.valid_table_name? ( s )
      s =~ /\A[a-zA-Z][a-zA-Z0-9]{2,62}\z/
    end

    def self.alphanumeric? ( s )
      s =~ /\A[a-zA-Z0-9]*\z/
    end
    
    def self.valid_file_path
      s =~ /\A(?:[a-zA-Z]\:|\\\\[\w\.]+\\[\w.$]+)\\(?:[\w]+\\)*\w([\w.])+\z/
    end

    def self.ext? ( s )
      s =~ /\A[a-zA-Z0-9\-\_]*\z/
    end

    def self.base64? ( s )
      s =~ /\A(?:[A-Za-z0-9\+\/]{4})*(?:[A-Za-z0-9\+\/]{2}==|[A-Za-z0-9\+\/]{3}\=)?\z/
    end

    def self.url? ( s )
      s =~ /\A#{URI::regexp(['http', 'https'])}\z/
    end

    def self.hostname? ( s )
      s =~ /\A(?<hostname>([A-Za-z0-9\.\-]+)|\[[0-9A-Fa-f\:]+\])(:(?<port>\d+))?\z/
    end
 
    def self.to_storage_name ( s )
      s.nil? ? nil : s.downcase.gsub(/[^0-9a-z]/i, '')
    end
  end

end
