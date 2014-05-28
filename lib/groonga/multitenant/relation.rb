module Groonga
  module Multitenant
    class Relation
      include Enumerable

      def initialize(groonga, model)
        @groonga = groonga
        @model = model
        @params = {}
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

      private
      def records
        @groonga.select(table_name, @params)
      end

      def table_name
        @model.name.tableize
      end
    end
  end
end
