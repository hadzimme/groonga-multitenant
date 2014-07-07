module Groonga
  module Multitenant
    class Base
      include ActiveModel::Model
      include ActiveModel::Validations
      include ActiveModel::Serializers::JSON

      DATA_TYPES = [
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

        def initialize(object, name, class_name)
          @items = object.instance_variable_get("@#{name}")
          case class_name
          when nil
            @klass = nil
            @object_filter = RAW_DATA
          when 'Time'
            @klass = nil
            @object_filter = TO_TIME
          else
            @klass = class_name.constantize
            @object_filter = TO_MODEL
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

        def initialize(object, range, groonga)
          @id = object.id
          @table = object.class.name.tableize
          @range = range
          @klass = range.classify.constantize
          @groonga = groonga
        end

        def each
          return self.to_enum { self.count } unless block_given?
          items = @groonga.select(@range, query: "#{@table}:@#{@id}")

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
          column_list = groonga.column_list(table_name)

          column_list.select do |column|
            column[:flags][/PERSISTENT/]
          end.each do |column|
            case column[:flags]
            when /COLUMN_SCALAR/
              define_scalar_method(column[:name], column[:range])
            when /COLUMN_VECTOR/
              define_vector_method(column[:name], column[:range])
            when /COLUMN_INDEX/
              define_index_method(column[:name], column[:range])
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

        def column_class_name(range)
          case range.intern
          when *DATA_TYPES
            nil
          when :Time
            'Time'
          else
            range.classify
          end
        end

        def define_scalar_method(name, range)
          case class_name = column_class_name(range)
          when nil
            attr_accessor name
          when 'Time'
            define_time_range_method(name)
          else
            define_method("#{name}=") do |item|
              klass = class_name.constantize
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
              klass = class_name.constantize
              klass.find(instance_variable_get("@#{name}"))
            end
          end
        end

        def define_vector_method(name, range)
          class_name = column_class_name(range)
          attr_writer name

          define_method(name) do
            VectorColumn.new(self, name, class_name)
          end
        end

        def define_index_method(name, range)
          attr_writer name

          define_method(name) do
            IndexColumn.new(self, range, groonga)
          end
        end

        def define_time_range_method(name)
          attr_writer name

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

        columns.select do |column|
          column[:flags][/COLUMN_VECTOR/]
        end.each do |column|
          hash[column[:name]] = instance_variable_get("@#{column[:name]}")
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

        columns.select do |column|
          column[:range].intern == :Time
        end.each do |column|
          value = instance_variable_get("@#{column[:name]}")
          timestamp[column[:name]] = value
        end

        hash = __as_json(options).merge(timestamp)

        columns.select do |column|
          column[:flags][/COLUMN_VECTOR/]
        end.each do |column|
          hash[column[:name]] = instance_variable_get("@#{column[:name]}")
        end

        keys_to_reject = columns.select do |column|
          column[:flags][/COLUMN_INDEX/]
        end.map do |column|
          column[:name]
        end + ['_id']

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
