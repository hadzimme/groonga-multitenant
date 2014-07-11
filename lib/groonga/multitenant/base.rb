module Groonga
  module Multitenant
    class Base
      include ActiveModel::Model
      include ActiveModel::Validations
      include ActiveModel::Serializers::JSON

      class VectorColumn
        include Enumerable

        RAW_DATA = lambda do |item, _|
          item
        end

        TO_TIME = lambda do |item, _|
          time = Time.at(item)
          if timezone = Time.zone
            time.getlocal(timezone.formatted_offset)
          else
            time
          end
        end

        TO_MODEL = lambda do |item, klass|
          klass.find(item)
        end

        def initialize(object, column)
          @items = object.instance_variable_get("@#{column.name}")
          case column.range_type
          when :built_in
            @object_filter = RAW_DATA
          when :time
            @object_filter = TO_TIME
          when :reference
            @klass = column.classified_range
            @object_filter = TO_MODEL
          else
            raise 'There is something wrong...'
          end
        end

        def each
          return self.to_enum { @items.size } unless block_given?

          @items.each do |item|
            yield @object_filter.call(item, @klass)
          end

          self
        end

        def size
          @items.size
        end
      end

      class IndexColumn
        include Enumerable

        def initialize(object, column, groonga)
          @groonga = groonga
          @range = column.range
          source_column = column.source.first.split('.').last
          @query = "#{source_column}:@#{object.id}"
          @klass = column.classified_range
        end

        def each
          return self.to_enum { self.count } unless block_given?
          items = @groonga.select(@range, query: @query)

          items.each do |params|
            yield @klass.new(params)
          end

          self
        end

        def size
          self.count
        end
      end

      class << self
        def configure(params)
          @@groonga = Groonga::Multitenant::Client.new(params)
          self
        end

        def inherited(subclass)
          subclass.define_column_based_methods
        end

        def define_column_based_methods
          columns = groonga.column_list(table_name)

          columns.select(&:persistent?).each do |column|
            case column.flags
            when /COLUMN_SCALAR/
              define_scalar_method(column)
            when /COLUMN_VECTOR/
              define_vector_method(column)
            when /COLUMN_INDEX/
              define_index_method(column)
            end
          end
        end

        def where(params)
          #TODO Relation.new(@@groonga, self).where(params)
        end

        def all
          Relation.new(@@groonga, self)
        end

        def find(id)
          records = groonga.select(table_name, filter: "_key == #{id}")
          raise 'record not found' unless records.first
          self.new(records.first)
        end

        def count
          groonga.select(table_name, limit: 0).count
        end

        private
        def groonga
          @@groonga
        rescue NameError
          raise 'groonga client not configured'
        end

        def table_name
          self.name.tableize
        end

        def define_scalar_method(column)
          case column.range_type
          when :built_in
            attr_accessor column.name
          when :time
            define_time_range_method(column.name)
          when :reference
            klass = column.classified_range
            name = column.name

            define_method("#{name}=") do |item|
              case item
              when klass
                instance_variable_set("@#{name}", item.id)
              when Integer
                instance_variable_set("@#{name}", item)
              else
                raise TypeError, "should be #{klass} or Integer"
              end
            end

            define_method(name) do
              klass.find(instance_variable_get("@#{name}"))
            end
          else
            raise 'There is something wrong...'
          end
        end

        def define_vector_method(column)
          name = column.name
          attr_writer name

          define_method(name) do
            VectorColumn.new(self, column)
          end
        end

        def define_index_method(column)
          name = column.name
          attr_writer name

          define_method(name) do
            IndexColumn.new(self, column, groonga)
          end
        end

        def define_time_range_method(name)
          define_method("#{name}=") do |time|
            case time
            when Time, Integer
              instance_variable_set("@#{name}", time.to_f)
            when Float
              instance_variable_set("@#{name}", time)
            else
              raise TypeError, 'should be Time, Integer or Float'
            end
            time
          end

          define_method(name) do
            sec = instance_variable_get("@#{name}")
            return unless sec
            time = Time.at(sec)
            if timezone = Time.zone
              time.getlocal(timezone.formatted_offset)
            else
              time
            end
          end
        end
      end

      attr_accessor :_id, :_key
      alias id _key
      alias __as_json as_json
      private :__as_json

      def persisted?
        !@_key.nil?
      end

      def save
        return false unless self.valid?
        @_key ? update : create
        self
      end

      def attributes
        instance_values
      end

      def as_json(options = nil)
        hash = __as_json(options)
        columns = groonga.column_list(table_name)

        columns.select(&:vector?).each do |column|
          name = column.name
          hash[name] = instance_variable_get("@#{name}")
        end

        hash['id'] = @_key
        hash.reject { |key, _| key[/^_/] }
      end

      private
      def update
        update_timestamps
        groonga.load(values, table_name)
      end

      def create
        create_timestamps
        table = table_name
        @_key = groonga.max_key(table) + 1
        groonga.load(values, table)
      end

      def update_timestamps
        if self.respond_to?(:updated_at)
          @updated_at = Time.new.to_f
        end
      end

      def create_timestamps
        if self.respond_to?(:created_at) && self.respond_to?(:updated_at)
          @created_at = @updated_at = Time.new.to_f
        end
      end

      def to_load_json(options = nil)
        columns = groonga.column_list(table_name)
        timestamp = {}

        columns.select(&:time?).each do |column|
          name = column.name
          value = instance_variable_get("@#{name}")
          timestamp[name] = value
        end

        hash = __as_json(options).merge(timestamp)

        columns.select(&:vector?).each do |column|
          name = column.name
          hash[name] = instance_variable_get("@#{name}")
        end

        keys_to_reject = columns.select(&:index?).map(&:name) + ['_id']
        hash.reject { |key, _| keys_to_reject.include?(key) }.to_json
      end

      def values
        "[#{to_load_json}]"
      end

      def groonga
        @@groonga
      rescue NameError
        raise 'groonga client not configured'
      end

      def table_name
        self.class.name.tableize
      end
    end
  end
end
