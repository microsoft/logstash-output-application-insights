Gem::Specification.new do |s|
  s.name          = 'logstash-output-msai'
  s.version       = "0.1.0"
  s.licenses      = ["Apache License (2.0)"]
  s.summary       = "Send diagnostic data to Microsoft Application Analytics."
  s.description   = "This gem is a Logstash plugin. It send diagnostic data to Azure Application Analytics. Use Blob + Notification"
  s.authors       = ["Microsoft Corporation"]
  s.email         = "michabin@microsoft.com"
  s.homepage      = "http://www.microsoft.com"
  s.require_paths = ["lib"]

  # Files
  s.files         = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files    = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata      = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core", ">= 2.0.0", "< 3.0.0"
  s.add_runtime_dependency "azure-storage"
  s.add_runtime_dependency "azure-core"
  s.add_runtime_dependency "logstash-codec-plain"
  s.add_development_dependency "logstash-devutils"
end
