# encoding: utf-8

module LogStash; module Outputs; class Msai;
  class Notify

    public

    def self.config ( configuration )
      @@configuration = configuration
      @@logger = configuration[:logger]
      @@io_retry_delay = configuration[:io_retry_delay]
      @@io_max_retries = configuration[:io_max_retries]

      # queue for failed notifications, that we will continue to try resending, should have a plolicy when it continues to grow
      @@failed_to_notify_retry_Q = Queue.new

      # a thread that retries to notify, notifications that failed in the past
      thread = Thread.new do
        loop do
          tuple = @@failed_to_notify_retry_Q.pop
          unless Notify.new( tuple ).notify
            sleep( 60 )
          end
        end
      end

    end

    def initialize ( blob_or_tuple )
      if blob_or_tuple.is_a? Blob
        @ikey = blob_or_tuple.ikey
        @schema = blob_or_tuple.schema
        @storage_account_name = blob_or_tuple.storage_account_name
        @container_name = blob_or_tuple.container_name
        @blob_name = blob_or_tuple.blob_name
        @uploaded_events_count = blob_or_tuple.uploaded_events_count
        @uploaded_size = blob_or_tuple.uploaded_size
        @oldest_event_time = blob_or_tuple.oldest_event_time
        @max_tries = @@io_max_retries + 1
      else
        ( @start_time, parameters ) = blob_or_tuple
        ( action, @ikey, @schema, @storage_account_name, @container_name, @blob_name, @uploaded_events_count, @uploaded_size, @oldest_event_time ) = parameters
      end
    end

    def async_notify
      # create a thread that handles the IO to notify Breeze
      thread = Thread.new do
        notify
      end
    end

    def notify
      @try_count = 1

      begin
        @client ||= Client.new( @storage_account_name )

        set_blob_sas_url
        info = "notify #{@storage_account_name}/#{@container_name}/#{@blob_name}, events: #{@uploaded_events_count}, size: #{@uploaded_size}, , delay: #{Time.now.utc - @oldest_event_time}, blob_sas_url: #{@blob_sas_url}"

        payload = create_payload
        @@logger.debug { "notification payload: #{payload}" }

        unless @@configuration[:disable_notification]
          post_notification( @client.notifyClient, payload ) unless @@configuration[:disable_notification]
        else
          info = "DISABLED #{info}"
        end

        @@logger.info { "Successed to #{info}" }
        true

      rescue StandardError => e
        parameters ||= [ "notify", @ikey, @schema, @storage_account_name, @container_name, @blob_name, @uploaded_events_count, @uploaded_size, @oldest_event_time ]
        tuple = [ @start_time || Time.now.utc, parameters ]
            
        recovery = recover( info, @try_count, @max_tries, parameters, e )
        if recovery
          if recovery.is_a?( Fixnum )
            try_count = recovery
          end
          retry
        end
        false

      ensure
        @client = @client.dispose if @client
      end
    end

    private

    def recover ( info, try_count, max_tries, parameters, e )

      tuple = [ @start_time || Time.now.utc, parameters ]

      retry_later = false

      if e.is_a?( Azure::Core::Http::HTTPError )

        if 404 == e.status_code && "ContainerNotFound" == e.type
          @@logger.error { "Failed to #{info} ;( recovery: create container, error: #{e.inspect}" }
          return :create_container if "upload" == parameters[0]
        elsif 409 == e.status_code && "ContainerAlreadyExists" == e.type
          @@logger.error { "Failed to #{info} ;( recovery: continue, error: #{e.inspect}" }
          return :continue
        elsif 403 == e.status_code && "AuthenticationFailed" == e.type
          if @client.switch_storage_account_key!
            @@logger.error { "Failed to #{info} ;( recovery: switched to secondary storage key, error: #{e.inspect}" }
            return :invalid_storage_key
          end
          retry_later = true

        # Auth error, Notify
        elsif 403 == e.status_code  && "Unknown" == e.type && e.description.include?("Blob does not exist or not accessible.")
          if @client.switch_storage_account_key!
            @@logger.error { "Failed to #{info} ;( recovery: switched to secondary storage key, error: #{e.inspect}" }
            return :invalid_storage_key
          end
          retry_later = true
        end

      elsif e.is_a?( Faraday::ConnectionFailed ) || e.is_a?( Faraday::TimeoutError )

        if try_count < max_tries
          @client = @client.dispose  if @client
      
          @@logger.error { "Failed to #{info} ;( recovery: retry, try #{try_count} / #{max_tries}, error: #{e.inspect}" }
          sleep( @@io_retry_delay )
          return  try_count.next
        end

        retry_later = true
      end

      if retry_later
        @@logger.error { "Failed to #{info} ;( recovery: retry later,  try #{try_count} / #{max_tries}, error: #{e.inspect}" }
        if "commit" == parameters[0]
          @@failed_on_commit_retry_Qs[3] << tuple
        elsif "upload" == parameters[0]
          @failed_on_upload_retry_Q << tuple
        elsif "notify" == parameters[0]
          @@failed_to_notify_retry_Q << tuple
        else
          @@logger.error { "Failed to #{info} :( recovery: nonrecoverable, error: #{e.inspect}" }
          @@configuration[:failed_on_action_nonrecoverable_Q] << tuple
        end
      else
        @@logger.error { "Failed to #{info} :( recovery: nonrecoverable, error: #{e.inspect}" }
        @@configuration[:failed_on_action_nonrecoverable_Q] << tuple
      end

      nil

      # Blob service error codes - msdn.microsoft.com/en-us/library/azure/dd179439.aspx
      # ConnectionFailed - problem with connection
      # ParsingError - problem with request/response payload
      # ResourceNotFound, SSLError, TimeoutError

      # Unknown (403): {"itemsReceived":1,"itemsAccepted":0,"errors":[{"index":0,"statusCode":403,"message":"Blob does not exist or not accessible."}]}
      # wrong key - Unknown (400): {"itemsReceived":1,"itemsAccepted":0,"errors":[{"index":0,"statusCode":400,"message":"Invalid instrumentation key"}]}
      # wrong schema - No error !!!
      # Missing field time from payload - Unknown (400): {"itemsReceived":1,"itemsAccepted":0,"errors":[{"index":0,"statusCode":400,"message":"109: Field 'time' on type 'Envelope' is required but missing or empty. Expected: string, Actual: undefined"}]}
      # valid url such as "http://www.google.com", (400)
      # ... CHECK WAHT OTHER ERRORS EXIST
      # HttpError has following members: [:@status_code, :@http_response, :@uri, :@description, :@type]
    end


    def create_payload
      notification_hash = {
        :data => {
          :baseType => DATA_BASE_TYPE,
          :baseData => {
            :ver           => BASE_DATA_REQUIRED_VERSION,
            :blobSasUri    => @blob_sas_url.to_s,
            :sourceName    => @schema,
            :sourceVersion => @@configuration[:notification_version].to_s
          }
        }, 
        :ver  => @@configuration[:notification_version], 
        :name => REQUEST_NAME,
        :time => Time.now.utc.iso8601,
        :iKey => @ikey
      }
      notification_hash.to_json
    end 

    def post_notification ( http_client, body )
      request = Azure::Core::Http::HttpRequest.new( :post, @@configuration[:notification_endpoint], { :body => body, :client => http_client } )
      request.headers['Content-Type'] = 'application/json; charset=utf-8'
      request.headers['Accept'] = 'application/json'
      @@logger.debug { "send notification : \n    endpoint: #{@@configuration[:notification_endpoint]}\n    body : #{body}" }
      response = request.call
    end 

    def set_blob_sas_url
      blob_url ="https://#{@storage_account_name}.blob.core.windows.net/#{@container_name}/#{@blob_name}"
      options_and_constrains = {:permissions => "r", :resource => "b", :expiry => ( Time.now.utc + @@configuration[:blob_access_expiry_time] ).iso8601 }
      @blob_sas_url = @client.storage_auth_sas.signed_uri( URI( blob_url ), options_and_constrains )
    end


  end
end end end
