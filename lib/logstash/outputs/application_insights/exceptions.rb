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

  class NotRecoverableError < StandardError
  end

  class AssertError < StandardError
  end

  # exception that cause process to end
  # LogStash::ConfigurationError, "ssl_truststore_location must be set when SSL is enabled"
  # class ConfigurationError < StandardError
  class ConfigurationError < LogStash::ConfigurationError
  end

  class UnexpectedBranchError < StandardError
  end

end
