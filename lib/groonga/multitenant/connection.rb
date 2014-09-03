module Groonga
  module Multitenant
    class Connection
      class ResponseError < Standard::Error
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
        response = Groonga::Client.open do |client|
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
    end
  end
end
