# encoding: utf-8

class LogStash::Outputs::Msai
  class State

    public

    def initialize
      @bytes_in_memory = Concurrent::AtomicFixnum.new(0)
      @pending_commits = Concurrent::AtomicFixnum.new(0)
      @pending_notifications = Concurrent::AtomicFixnum.new(0)
    end


    def bytes_in_memory
      @bytes_in_memory.value
    end


    def pending_commits
      @pending_commits.value
    end


    def pending_notifications
      @pending_notifications.value
    end


    def inc_upload_bytesize ( bytesize )
      @bytes_in_memory.increment( bytesize )
    end


    def dec_upload_bytesize ( bytesize )
      @bytes_in_memory.decrement( bytesize )
    end


    def inc_pending_commits
      @pending_commits.increment
    end


    def dec_pending_commits
      @pending_commits.decrement
    end


    def inc_pending_notifications
      @pending_notifications.increment
    end


    def dec_pending_notifications
      @pending_notifications.decrement
    end

    public

    @@instance = State.new

    def self.instance
      @@instance
    end

    private_class_method :new
  end
end

