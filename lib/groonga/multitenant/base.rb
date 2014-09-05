module Groonga
  module Multitenant
    class RecordInvalid < StandardError
    end

    class RecordNotFound < StandardError
    end

    class Base
      include ActiveModel::Model
      include ActiveModel::Validations
      include ActiveModel::Serializers::JSON

      class << self
        def establish_connection(params = {})
          @@connection = Connection.new(params)
        end

        def inherited(subclass)
          return if subclass.name.nil?
          subclass.define_column_based_methods
        end

        def define_column_based_methods
          @@columns = @@connection.column_list(self.name)
          @@time_columns = @@columns.select{|c| c.range == 'Time' }
          @@value_columns = @@columns.reject{|c| c.flags[/COLUMN_INDEX/] }
          @@index_columns = @@columns.select{|c| c.flags[/COLUMN_INDEX/] }
          @@index_column_names = @@index_columns.map(&:name)

          @@columns.select{|c| c.flags[/PERSISTENT/] }.each do |column|
            define_column_based_method(column)
          end
        end

        def where(params)
          Relation.new(@@connection, self).where(params)
        end

        def select(*columns)
          Relation.new(@@connection, self).select(*columns)
        end

        def limit(num)
          Relation.new(@@connection, self).limit(num)
        end

        def offset(num)
          Relation.new(@@connection, self).offset(num)
        end

        def drilldown(columns)
          Relation.new(@@connection, self).drilldown(columns)
        end

        def all
          Relation.new(@@connection, self)
        end

        def find(id)
          response = @@connection.select(self.name, query: "id:#{id}")
          unless record = response.records.first
            raise RecordNotFound, 'Record not found', caller
          end
          self.new(record)
        end

        def count
          @@connection.select(self.name, limit: 0).n_hits
        end

        def import(ary)
          unless ary.all?{|item| item.instance_of?(self) }
            raise "All objects should be `#{self}'"
          end
          raise 'There are some invalid objects' unless ary.all?(&:valid?)
          first_id = max_id + 1
          time = Time.new.to_f
          timestamp = { 'created_at' => time, 'updated_at' => time }

          values = ary.map.with_index do |item, index|
            id = first_id + index
            params = timestamp.merge('_key' => id, 'id' => id)
            item.as_value.merge(params)
          end

          @@connection.load(values.to_json, self.name)
          values.size
        end

        def max_id
          @@connection.select(id_table, limit: 0).n_hits
        end

        private
        def id_table
          "#{self}Id"
        end

        def define_column_based_method(column)
          if column.range == 'Time'
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
      alias __as_json as_json
      private :__as_json

      def persisted?
        !@_key.nil?
      end

      def destroy
        unless @_key.nil?
          @@connection.delete(self.class.name, key: @_key)
        end
      end

      def attributes
        instance_values
      end

      def as_json(options = nil)
        params = __as_json(options)

        params.reject do |key, _|
          @@index_column_names.include?(key) || key[/^_/]
        end
      end

      def as_value
        params = __as_json(nil)

        params.reject do |key, _|
          @@index_column_names.include?(key) || key == '_id'
        end.merge(raw_timestamp)
      end

      def update_attributes(params)
        params.each do |key, value|
          self.public_send("#{key}=", value)
        end

        self.save
      end

      def save
        update_metadata
        unless @_key == @id
          raise RecordInvalid, 'Id should not be modified', caller
        end
        @@connection.load([as_value].to_json, self.class.name)
        params = @@connection.select(self.class.name, query: "id:#{@id}").records.first

        params.each do |attr, value|
          self.public_send("#{attr}=", value)
        end

        self
      end

      private
      def update_metadata
        @updated_at = Time.new.to_f
        if @_key.nil?
          @created_at = @updated_at
          @_key = @id = max_id + 1
        end
      end

      def max_id
        self.class.max_id
      end

      def raw_timestamp
        @@time_columns.reduce({}) do |result, column|
          key = column.name
          value = instance_variable_get("@#{key}")
          result.merge(key => value)
        end
      end
    end
  end
end
