module Groonga
  module Multitenant
    class ParamInvalid < StandardError
    end

    class Relation
      include Enumerable

      def initialize(groonga, model)
        @groonga = groonga
        @model = model
        @columns = []
        @params = { limit: -1 }
        @order = []
      end

      def each
        return self.to_enum { records.size } unless block_given?

        records.each do |record|
          yield @model.new(record)
        end

        self
      rescue Connection::InvalidArgument
        raise ParamInvalid, 'Invalid parameters', caller
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

      def offset(num)
        @params.merge!(offset: num)
        self
      end

      def order(*columns)
        @order.concat(columns)
        self
      end

      def count
        @groonga.select(@params.merge(limit: 0, table: @model.name)).n_hits
      end

      def drilldown
        @groonga.select(@params.merge(limit: 0, table: @model.name)).drilldown
      end

      def exist?
        @groonga.select(@params.merge(limit: 0, table: @model.name)).n_hits > 0
      end

      def empty?
        @groonga.select(@params.merge(limit: 0, table: @model.name)).n_hits == 0
      end

      def to_json
        self.to_a.to_json
      end

      private
      def records
        unless @order.empty?
          @params[:sortby] = @order.join(',')
        end
        if @columns.empty?
          @params[:output_columns] = '_id,_key,*'
        else
          @params[:output_columns] = "_id,_key,#{@columns.join(',')}"
        end
        @groonga.select(@params.merge(table: @model.name))
      end
    end
  end
end
