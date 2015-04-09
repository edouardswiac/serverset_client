# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "serverset_client/version"

Gem::Specification.new do |s|
  s.name        = "serverset_client"
  s.version     = ServersetClient::VERSION
  s.authors     = ["Dave B."]
  s.email       = ["edouard.swiac[at]gmail.com"]
  s.summary     = 'JRuby compatible Serverset client library'
  s.description = s.summary

  s.files         = Dir.glob("{lib}/**/*") + %w(README.md)
  s.test_files    = Dir.glob("{test}/**/*_test.rb")
  s.executables   = Dir.glob("{bin}/*").map { |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_runtime_dependency 'thrift', '>= 0.2.0'
  s.add_runtime_dependency 'thrift_client', '>= 0.6.0'
  s.add_runtime_dependency 'zookeeper', '~> 1.4.10'
  s.add_runtime_dependency 'zk', '>= 1.7.0'
  s.add_runtime_dependency 'multi_json'
end
