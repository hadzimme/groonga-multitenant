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

        def initialize(object, name, range)
        end

        def each
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

        def define_scalar_method(name, range)
          case range.intern
          when *DATA_TYPES
            attr_accessor name
          when :Time
            define_time_range_method(name)
          else
            define_method("#{name}=") do |item|
              klass = range.classify.constantize
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
              klass = range.classify.constantize
              klass.find(instance_variable_get("@#{name}"))
            end
          end
        end

        def define_vector_method(name, range)
          attr_writer name

          define_method(name) do
            VectorColumn.new(self, name, range)
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

      def created_at
        return nil unless @created_at
        time = Time.at(@created_at)
        if tz = Time.zone
          time.getlocal(tz.formatted_offset)
        else
          time
        end
      end

      def updated_at
        return nil unless @updated_at
        time = Time.at(@updated_at)
        if tz = Time.zone
          time.getlocal(tz.formatted_offset)
        else
          time
        end
      end

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
        hash['id'] = @_key
        hash.reject { |key, _| key[/^_/] }
      end

      private
      def update
        @updated_at = Time.new.to_f
        groonga.load(values, table_name)
      end

      def create
        @created_at = @updated_at = Time.new.to_f
        table = table_name
        @_key = groonga.max_key(table) + 1
        groonga.load(values, table)
      end

      def to_load_json(options = nil)
        timestamps = {
          'created_at' => @created_at,
          'updated_at' => @updated_at,
        }
        hash = __as_json(options).merge(timestamps)
        hash.reject { |key, _| key == '_id' }.to_json
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
