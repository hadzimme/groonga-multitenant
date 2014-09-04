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
        return self.to_enum { self.count } unless block_given?
        response = execute_command

        response.records.each do |record|
          yield @model.new(record)
        end
      rescue Connection::ResponseError
        raise ParamInvalid, 'Invalid parameters', caller
      else
        self
      end

      def size
        self.count
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

      def drilldown(columns)
        @params.merge!(drilldown: colomns)
        self
      end

      def order(*columns)
        @order.concat(columns)
        self
      end

      def n_hits
        @groonga.select(@model.name, @params.merge(limit: 0)).n_hits
      end

      def drilldown
        @groonga.select(@model.name, @params.merge(limit: 0)).drilldown
      end

      def exist?
        @groonga.select(@model.name, @params.merge(limit: 0)).n_hits > 0
      end

      def empty?
        @groonga.select(@model.name, @params.merge(limit: 0)).n_hits == 0
      end

      def to_json
        self.to_a.to_json
      end

      private
      def execute_command
        unless @order.empty?
          @params[:sortby] = @order.join(',')
        end
        if @columns.empty?
          @params[:output_columns] = '_id,_key,*'
        else
          @params[:output_columns] = "_id,_key,#{@columns.join(',')}"
        end
        @groonga.select(@model.name, @params)
      end
    end
  end
end
