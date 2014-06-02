require 'active_model'

module Groonga
  module Multitenant
    class Tenant
      include ActiveModel::Model

      class << self
        def current
          Thread.current[:tenant]
        end

        def current=(tenant)
          Thread.current[:tenant] = tenant
        end
      end

      attr_accessor :code
    end
  end
end
