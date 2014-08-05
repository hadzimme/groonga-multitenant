module Groonga
  module Multitenant
    class Base
      include ActiveModel::Model
      include ActiveModel::Validations
      include ActiveModel::Serializers::JSON

      class << self
        def establish_connection(spec = {})
          @@spec = spec
        end

        def flags=(flags)
          @@flags = flags
        end

        def inherited(subclass)
          subclass.define_column_based_methods
        end

        def define_column_based_methods
          columns.select(&:persistent?).each do |column|
            define_column_based_method(column)
          end

          nil
        end

        def where(params)
          #TODO Relation.new(groonga, self).where(params)
        end

        def all
          Relation.new(groonga, self)
        end

        def find(id)
          records = groonga.select(self.name, filter: "_key == #{id}")
          raise 'record not found' unless records.first
          self.new(records.first)
        end

        def count
          groonga.select(self.name, limit: 0).count
        end

        def columns
          @@columns ||= groonga.column_list(self.name)
        end

        def index_columns
          @@index_columns ||= self.columns.select(&:index?)
        end

        private
        def spec
          @@spec ||= {}
        end

        def groonga
          @@groonga ||= Connection.new(spec)
        end

        def define_column_based_method(column)
          if column.time?
            define_time_range_method(column.name)
          else
            attr_accessor column.name
          end

          nil
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

          nil
        end
      end

      attr_accessor :_id, :_key
      alias id _id
      alias key _key
      alias __as_json as_json

      def key=(arg)
        @_key = arg
      end

      def persisted?
        !@_id.nil?
      end

      def save
        return false unless self.valid?
        @_key ? update : create
        self
      end

      def attributes
        instance_values
      end

      private
      def update
        @updated_at = Time.new.to_f
        groonga.load(value, self.class.name)
      end

      def create
        @created_at = @updated_at = Time.new.to_f
        groonga.load(value, self.class.name)
      end

      def json_to_load
        timestamp = {}

        columns.select(&:time?).each do |column|
          name = column.name
          value = instance_variable_get("@#{name}")
          timestamp[name] = value
        end

        hash = self.as_json.merge(timestamp)

        keys_to_reject = index_columns.map(&:name) + ['_id']
        hash.reject { |key, _| keys_to_reject.include?(key) }.to_json
      end

      def value
        "[#{json_to_load}]"
      end

      def groonga
        @@groonga
      end

      def columns
        self.class.columns
      end

      def index_columns
        self.class.index_columns
      end
    end
  end
end
