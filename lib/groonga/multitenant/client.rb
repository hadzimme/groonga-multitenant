module Groonga
  module Multitenant
    class Client
      class ColumnList
        include Enumerable
        KEYS = [
          :id,
          :name,
          :path,
          :type,
          :flags,
          :domain,
          :range,
          :source,
        ].freeze

        def initialize(ary)
          @columns = ary[1..-1].map do |values|
            Hash[KEYS.zip(values)]
          end
        end

        def each(&block)
          return self.to_enum { @columns.size } unless block_given?
          @columns.each(&block)
          self
        end

        def size
          @columns.size
        end
      end

      class Select
        include Enumerable
        attr_reader :count

        def initialize(response_body)
          count, columns, *rows = response_body[0]
          @count = count[0]
          keys = columns.map { |column| column.first.intern }

          @records = rows.map do |values|
            Hash[keys.zip(values)]
          end
        end

        def each(&block)
          return self.to_enum { self.size } unless block_given?
          @records.each(&block)
          self
        end

        def size
          @records.size
        end
      end

      class Status
        include ActiveModel::Model
        attr_accessor :alloc_count, :starttime, :uptime, :version
        attr_accessor :n_queries, :cache_hit_rate, :command_version
        attr_accessor :default_command_version, :max_command_version
      end

      DEFAULT_OPTIONS = {
        host: '127.0.0.1',
        port: 10041,
      }

      def initialize(options = {})
        @options = DEFAULT_OPTIONS.merge(options)
      end

      def column_list(table_name)
        params = { table: table_name }
        response = execute(:column_list, params)
        ColumnList.new(response.body)
      end

      def load(values, table_name, params = {})
        params = params.merge(values: values, table: table_name)
        response = execute(:load, params)
        response.body
      end

      def select(table_name, params = {})
        params = params.merge(table: table_name)
        response = execute(:select, params)
        Select.new(response.body)
      end

      def status
        response = execute(:status)
        Status.new(response.body)
      end

      MAX_KEY_PARAMS = {
        output_columns: '_key',
        sortby: '-_key',
        limit: 1,
      }

      def max_key(table_name)
        response = execute(:select, MAX_KEY_PARAMS.merge(table: table_name))
        record = Select.new(response.body).first
        record ? record[:_key] : 0
      end

      private
      def execute(command, params = {})
        client = Groonga::Client.open(@options.merge(prefix: tenant.code))
        client.send(command, params)
      ensure
        client.close
      end

      def tenant
        unless tenant = Groonga::Multitenant::Tenant.current
          raise 'tenant undefined'
        end
        tenant
      end
    end
  end
end
