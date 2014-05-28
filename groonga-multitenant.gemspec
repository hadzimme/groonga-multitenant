# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'groonga/multitenant/version'

Gem::Specification.new do |spec|
  spec.name          = "groonga-multitenant"
  spec.version       = Groonga::Multitenant::VERSION
  spec.authors       = ["Hajime Wakahara"]
  spec.email         = ["hadzimme@icloud.com"]
  spec.summary       = %q{Thread-based multitenancy on Groonga}
  spec.description   = %q{Thread-based multitenancy on Groonga}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "activemodel", "~> 4.1"

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
end
