module Groonga
  module Multitenant
    class Fork
      def initialize(app)
        @app = app
      end

      def call(env)
        Thread.start(env) do |env|
          @app.call(env)
        end.value
      ensure
        GC.start
      end
    end
  end
end
