# Slow Query Parser

requires Ruby 2.0


### As a command-line tool

    ./slow_query_log_analyzer.rb \
       /data/fusionio/mysql/slow-query.log \
       "slower_than(5).drop(10).take(2)"

### In IRB

    require 'slow_query_log_analyzer'
    queries = SlowLogAnalyzer.new('/data/fusionio/mysql/slow-query.log').queries
    # Gated by a specific minimum slowness:
    queries.slower_than(5).first

    # From an interesting file:
    queries.from_file(/_job\.rb/).map {|q| q[:Query_time] }

    # An arbitrarily complex (still lazily-processed!) query to show
    # the mean time taken for any given line of source code
    queries.slower_than(5).select do |q|
       q[:source]
    end.from_file(/my_slow_controller.rb:123/).drop(5).
    group_by {|q| q[:source] }.map do |source, qq|
      qq.reduce(n: 0.1, total: 0, source: source) do |data, q|
        data.update n: data[:n]+1, total: data[:total]+q['Query_time']
      end
    end.sort_by {|data| data[:total]/data[:n] }.reverse.each do |data|
      puts "#{'%0.4f' % (data[:total]/data[:n])} #{data[:source]}"
    end

Patches welcome, forks celebrated.

Copyright 2016 Jack Danger Canty @ [https://jdanger.com](https://jdanger.com) released under the MIT license
