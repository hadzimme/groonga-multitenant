require 'active_model'

module Groonga
  module Multitenant
    class Tenant
      include ActiveModel::Model

      class << self
        def current
          Thread.current[:gm_tenant]
        end

        def current=(tenant)
          Thread.current[:gm_tenant] = tenant
        end
      end

      attr_accessor :code
    end
  end
end
