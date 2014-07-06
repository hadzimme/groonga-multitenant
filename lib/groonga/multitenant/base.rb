module Groonga
  module Multitenant
    class Base
      include ActiveModel::Model
      include ActiveModel::Validations
      include ActiveModel::Serializers::JSON

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
          p column_list
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
      end

      attr_accessor :_id, :_key
      attr_writer :created_at, :updated_at
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
