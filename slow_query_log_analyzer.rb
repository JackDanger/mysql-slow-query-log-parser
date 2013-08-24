#!/usr/bin/env ruby
# Usage (requires Ruby 2.0):
#
#   require 'slow_query_log_analyzer'
#   queries = SlowLogAnalyzer.new('/data/fusionio/mysql/slow-query.log').queries
#   # Gated by a specific minimum slowness:
#   queries.slower_than(5).first
#
#   # From an interesting file:
#   queries.from_file(/_job\.rb/).map {|q| q[:Query_time] }
#
#   # An arbitrarily complex (still lazily-processed!) query to show
#   # the mean time taken for any given line of source code
#   queries.slower_than(5).select do |q|
#      q[:source]
#   end.from_file(/my_slow_controller.rb:123/).drop(5).
#   group_by {|q| q[:source] }.map do |source, qq|
#     qq.reduce(n: 0.1, total: 0, source: source) do |data, q|
#       data.update n: data[:n]+1, total: data[:total]+q['Query_time']
#     end
#   end.sort_by {|data| data[:total]/data[:n] }.reverse.each do |data|
#     puts "#{'%0.4f' % (data[:total]/data[:n])} #{data[:source]}"
#   end
#
class SlowQueryLogAnalyzer
  ParserStates = [:loading, :query, :comment]
  Skip = /^(SET |SHOW |ANALYZE |USE )/i
  Comment = /^# /

  def initialize(filename)
    @filename = filename
    @state = :loading
  end

  module Query
    def from_file(pattern)
      pattern = /#{pattern}/ if pattern.is_a? String
      select {|query| query[:source] =~ pattern }
    end

    def slower_than(seconds)
      select do |query|
        query['Query_time'] > seconds
      end
    end

    require 'stringio'
    require 'pp'
    def to_s
      arr = to_a
      pp arr
      arr
    end

    def inspect
      #puts "Reader #{yielder.linecount} lines in #{"%0.2f" % (Time.now-start)} seconds"
      to_s
    end
  end

  Enumerator::Lazy.send :include, Query

  def queries
    reset!
    lazy do |yielder|
      File.foreach(@filename) do |l|

        line = l.chomp

        ## This is a shit parser.

        case @state
        # God knows what's at the start of the file.
        when :loading
          if line =~ Skip
            @state = :comment
          end
        # We're collecting the lines of a query
        when :query
          if line =~ Comment
            # we've finished parsing the query
            @state = :comment
            _comment << line
          elsif line =~ Skip
            # don't care about these statements
          else
            # keep parsing the query
            _query << line
          end
        # We're collecting the lines of data on a query
        when :comment
          # keep going?
          if line =~ Comment
            _comment << line
          # finished a comment block?
          else
            yielder.yield build_query(_query, _comment) if _query.any?
            reset!
            @state = :query
            _query << line unless line =~ Skip
          end
        end
      end
    end
  end

  private

  attr_accessor :_query, :_comment

  def lazy(&block)
    e = Enumerator.new(&block).lazy
    class << e
      attr_accessor :start, :linecount
      def each(*a)
        self.linecount += 1
        super
      end
    end
    e.start = Time.now
    e.linecount = 0
    e
  end

  def reset!
    @_query, @_comment = [], []
  end

  def build_query(sql, comment)
    comment.reduce(extract_sql(sql)) do |data, line|
      data.update extract_comment(line)
    end
  end

  def extract_sql(sql)
    parts = sql.join("\n").split(' -- ')
    parts.push(nil) if parts.size == 1
    {
      source: parts.pop,
      sql: parts.join(' -- ')
    }
  end

  def extract_comment(line)
    Hash[
      *line.split(/(\w+: )/).drop(1).map(&:strip).map{|l|l.chomp(':')}.map do |value|
        case value
        when /^\d+$/
          value.to_i
        when /^\d+\.?\d*$/
          value.to_f
        else
          value
        end
      end
    ]
  end
end

if __FILE__ == $0
  file, cmd = ARGV
  unless File.exist?(file) && cmd
    puts "Usage: slow_query_log_analyzer /path/to/slow-query.log \"from_file(/filename_pattern/).slower_than(2).first\""
  end
  p SlowQueryLogAnalyzer.new(file).queries.instance_eval(cmd)
end
