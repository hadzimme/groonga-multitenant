module Groonga
  module Multitenant
    class Connection
      COLUMN_KEYS = [
        :id,
        :name,
        :path,
        :type,
        :flags,
        :domain,
        :range,
        :source,
      ].freeze

      class Column
        BUILT_IN_TYPES = [
          :Bool,
          :Int8,
          :UInt8,
          :Int16,
          :UInt16,
          :Int32,
          :UInt32,
          :Int64,
          :UInt64,
          :Float,
          :ShortText,
          :Text,
          :LongText,
        ]

        include ActiveModel::Model
        attr_accessor *COLUMN_KEYS

        def persistent?
          !@flags[/PERSISTENT/].nil?
        end

        def vector?
          !@flags[/COLUMN_VECTOR/].nil?
        end

        def index?
          !@flags[/COLUMN_INDEX/].nil?
        end

        def time?
          @range == 'Time'
        end

        def range_type
          case @range.intern
          when *BUILT_IN_TYPES
            :built_in
          when :Time
            :time
          else
            :reference
          end
        end

        def classified_range
          case self.range_type
          when :reference
            @range.classify.constantize
          else
            nil
          end
        end
      end

      class ColumnList
        include Enumerable

        def initialize(ary)
          @columns = ary[1..-1].map do |values|
            Column.new(Hash[COLUMN_KEYS.zip(values)])
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
            Hash[keys.zip(values)].freeze
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
        STATUS_KEYS = [
          :alloc_count,
          :starttime,
          :uptime,
          :version,
          :n_queries,
          :cache_hit_rate,
          :command_version,
          :default_command_version,
          :max_command_version,
        ].freeze

        include ActiveModel::Model
        attr_accessor *STATUS_KEYS
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
        p response
        ColumnList.new(response.body)
      end

      def load(values, table_name, params = {})
        params = params.merge(values: values, table: table_name)
        response = execute(:load, params)
        response.body
      end

      def select(table_name, params = {})
        params = params.merge(table: table_name)
        p params
        response = execute(:select, params)
        Select.new(response.body)
      end

      def status
        response = execute(:status)
        Status.new(response.body)
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
