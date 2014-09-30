module Groonga
  module Multitenant
    module Pagination
      def current_page
        return 0 if n_records == 0
        @params[:offset] / @params[:limit] + 1
      end

      def n_pages
        (n_records.to_f / page_size).ceil
      end

      def n_records
        response.n_hits
      end

      def page_size
        @params[:limit]
      end

      def start_offset
        return 0 if n_records == 0
        @params[:offset] + 1
      end

      def end_offset
        return 0 if n_records == 0
        @params[:offset] + @params[:limit]
      end

      def prev_page
        first_page? ? nil : current_page - 1
      end

      def next_page
        last_page? ? nil : current_page + 1
      end

      def statistic
        {
          current_page: current_page,
          n_pages: n_pages,
          n_records: n_records,
          start_offset: start_offset,
          end_offset: end_offset,
          prev_page: prev_page,
          next_page: next_page,
        }
      end

      def first_page?
        current_page < 2
      end

      def last_page?
        current_page == n_pages
      end
    end
  end
end
