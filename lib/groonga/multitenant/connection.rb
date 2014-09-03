module Groonga
  module Multitenant
    class Connection
      class ResponseError < StandardError
      end

      class TenantMissing < StandardError
      end

      def initialize(params = {})
        @config = params
      end

      def column_list(table)
        execute(:column_list, table: table)
      end

      def delete(table, params = {})
        execute(:delete, params.merge(table: table))
      end

      def load(values, table, params = {})
        execute(:load, params.merge(values: values, table: table))
      end

      def select(table, params = {})
        execute(:select, params.merge(table: table))
      end

      private
      def execute(command, params = {})
        config = @config.merge(prefix: tenant.code)

        response = Groonga::Client.open(config) do |client|
          client.public_send(command, params)
        end

        case response
        when Groonga::Client::Response::Error
          _c, _ut, _et, message = response.header
          raise ResponseError, "Groonga returned: `#{message}'", caller(1)
        else
          response
        end
      end

      def tenant
        unless current_tenant = Tenant.current
          raise TenantMissing, 'Tenant should be set', caller(2)
        end
        current_tenant
      end
    end
  end
end
