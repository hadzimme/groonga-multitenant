module Groonga
  module Multitenant
    class ParamInvalid < StandardError
    end

    class Relation
      include Enumerable
      include Pagination

      def initialize(groonga, model)
        @groonga = groonga
        @model = model
        @columns = []
        @params = { limit: -1 }
        @order = []
      end

      def each
        return self.to_enum { self.count } unless block_given?

        response.records.each do |record|
          yield @model.new(record)
        end

        self
      end

      def where(params)
        @params.merge!(params)
        @response = nil
        self
      end

      def select(*columns)
        @columns.concat(columns)
        @response = nil
        self
      end

      def limit(num)
        @params.merge!(limit: num)
        @response = nil
        self
      end

      def offset(num)
        @params.merge!(offset: num)
        @response = nil
        self
      end

      def drilldown(columns)
        @params.merge!(drilldown: columns)
        @response = nil
        self
      end

      def drilldown_limit(num)
        @params.merge!(drilldown_limit: num)
        @response = nil
        self
      end

      def order(*columns)
        @order.concat(columns)
        @response = nil
        self
      end

      def n_hits
        response.n_hits
      end

      def drilldowns
        response.drilldowns
      end

      def to_json
        self.to_a.to_json
      end

      private
      def response
        @response ||= execute_command
      end

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
      rescue Connection::ResponseError
        raise ParamInvalid, 'Invalid parameters', caller(1)
      end
    end
  end
end
