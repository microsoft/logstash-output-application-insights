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
require 'date'
require File.expand_path('../lib/logstash/outputs/msai/version', __FILE__)

Gem::Specification.new do |s|
  s.name          = 'logstash-output-msai'
  s.version       = APPLICATION_INSIGHTS_VERSION
  s.licenses      = ["Apache License, Version 2.0"]
  s.summary       = "Microsoft Application Insights openschema Logstash output plugin. 'Output events to Application Insights"
  s.description   = "This gem is a Logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/logstash-plugin install gemname. This gem is not a stand-alone program"
  s.authors       = ["Microsoft Corporation"]
  s.email         = "michabin@microsoft.com"
  s.homepage      = "https://github.com/Microsoft/logstash-output-application-insights"
  s.require_paths = ["lib"]

  # Files
  s.files         = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','Rakefile','LICENSE','NOTICE.TXT','LICENSE.TXT']
   # Tests
  s.test_files    = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata      = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
#  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"
  s.add_runtime_dependency "logstash-core", ">= 2.0.0", "< 3.0.0"
  s.add_runtime_dependency "azure-storage", "0.10.1.preview"
  s.add_runtime_dependency "azure-core"
  # s.add_runtime_dependency "logstash-codec-plain"
  s.add_runtime_dependency "application_insights", ">= 0.5.3"
  s.add_development_dependency "logstash-devutils"
end
