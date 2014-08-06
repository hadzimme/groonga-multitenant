module Groonga
  module Multitenant
    class Relation
      include Enumerable

      def initialize(groonga, model)
        @groonga = groonga
        @model = model
        @columns = []
        @params = { limit: -1 }
      end

      def each
        return self.to_enum { records.size } unless block_given?

        records.each do |record|
          yield @model.new(record)
        end

        self
      end

      def size
        records.size
      end

      def where(params)
        @params.merge!(params)
        self
      end

      def select(*columns)
        @columns.concat(columns)
        self
      end

      def limit(num)
        @params.merge!(limit: num)
        self
      end

      private
      def records
        unless @columns.empty?
          @params[:output_columns] = "_id,_key,#{@columns.join(',')}"
        end
        @groonga.select(@model.name, @params)
      end
    end
  end
end
