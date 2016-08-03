# encoding: utf-8

class LogStash::Outputs::Msai

  # exception that are handled internally and do NOT cause process to end
  class BlockOverflowError < StandardError
  end

  class BlockTooSmallError < StandardError
  end

  class NoChannelError < StandardError
  end

  class ChannelExistError < StandardError
  end

  class StorageAccountsOffError < StandardError
  end

  class UploadRetryError < StandardError
  end

  # exception that cause process to end
  class ConfigurationError < StandardError
  end

  class UnexpectedBranchError < StandardError
  end

end
