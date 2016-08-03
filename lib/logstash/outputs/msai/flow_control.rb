# encoding: utf-8

class LogStash::Outputs::Msai
  class Flow_control

    public

    def initialize
      configuration = Config.current

      @logger_progname = configuration[:logger_progname]
      @logger = configuration[:logger]

      @flow_control_suspend_bytes = configuration[:flow_control_suspend_bytes]
      @flow_control_resume_bytes = configuration[:flow_control_resume_bytes]
      @flow_control_delay = configuration[:flow_control_delay]

      @flow_control_semaphore = Mutex.new

      @state = State.instance
    end


    def pass_or_wait
      bytes_in_memory = @state.bytes_in_memory
      return if bytes_in_memory <= @flow_control_resume_bytes

      @flow_control_semaphore.synchronize {
        bytes_in_memory = @state.bytes_in_memory
        if bytes_in_memory > @flow_control_suspend_bytes
          display_msg( "suspend receiving event, memory above #{@flow_control_suspend_bytes} bytes, wait till memory below #{@flow_control_resume_bytes} bytes" )
          loop do
            GC.start
            sleep( @flow_control_delay )
            bytes_in_memory = @state.bytes_in_memory
            break if bytes_in_memory <= @flow_control_resume_bytes
            display_msg( "continue to suspend receiving event, memory level #{bytes_in_memory} bytes, wait till memory below #{@flow_control_resume_bytes} bytes " )
          end
          display_msg( "resume receiving event, memory level #{bytes_in_memory}, below #{@flow_control_resume_bytes} bytes" )
        end
      }
    end

    private

    def display_msg ( msg )
        puts "+++ #{@logger_progname} #{msg}"
        # @logger.info { "#{msg}" }
    end

    public

    @@instance = Flow_control.new

    def self.instance
      @@instance
    end

    private_class_method :new
  end
end
