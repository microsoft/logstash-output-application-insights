# encoding: utf-8

class LogStash::Outputs::Msai
  class Shutdown

    public

    def initialize
      configuration = Config.current

      @logger_progname = configuration[:logger_progname]
      @logger = configuration[:logger]

      @state = State.instance
      @channels = Channels.instance
    end

    def submit
      display_msg( "start graceful shutdown, flush all events" )

      # wait for all uploads to finish
      start_bytes_in_memory = @state.bytes_in_memory
      bytes_in_memory = start_bytes_in_memory
      while bytes_in_memory > 0 do 
        sleep( 1 )
        bytes_in_memory = @state.bytes_in_memory
        percent = 100 * (1 - ( bytes_in_memory.to_f / start_bytes_in_memory ) )
        display_msg( "#{percent.to_i}% events were uploaded to Azure storage" ) if percent < 100.0
      end
      display_msg( "all events were uploaded to Azure storage" )

      Blob.close
      @channels.close

      # wait for all uploads to commit
      start_pending_commits = @state.pending_commits
      pending_commits = start_pending_commits
      while pending_commits > 0 do 
        sleep( 1 )
        pending_commits = @state.pending_commits
        percent = 100 * (1 - ( pending_commits.to_f / start_pending_commits ) )
        display_msg( "#{percent.to_i}% events were commited to Azure storage" ) if percent < 100.0
      end
      display_msg( "all events were commited to Azure storage" )

      # wait for all blobs to be notified
      start_pending_notifications = @state.pending_notifications
      pending_notifications = start_pending_notifications
      while pending_notifications > 0 do 
        sleep( 1 ) 
        pending_notifications = @state.pending_notifications
        percent = 100 * (1 - ( pending_notifications.to_f / start_pending_notifications ) )
        display_msg( "#{percent.to_i}% events were notified to Application Insights Analytics" ) if percent < 100.0
      end

      # done
      display_msg( "all events were notified to Application Insights Analytics" )
    end

    private

    def display_msg ( msg )
        puts "+++ #{@logger_progname} #{msg}"
        # @logger.info { "#{msg}" }
    end

    public

    @@instance = Shutdown.new

    def self.instance
      @@instance
    end

    private_class_method :new
  end
end
