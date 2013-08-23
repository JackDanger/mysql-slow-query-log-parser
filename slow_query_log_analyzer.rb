# Usage (requires Ruby 2.0):
#
#   require 'slow_query_log_analyzer'
#   queries = SlowQueryLogAnalyzer.new('/data/fusionio/mysql/slow-query.log')
#   queries.slower_than(5).select do |q|
#      q[:source]
#   end.take(3).map do |q|
#     q.select {|k,v| [:sql, :source, 'Query_time'].include?(k) }
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

  def from_file(source_snippet)
    queries.select {|query| query[:source] =~ /#{source_snippet}/ }
  end

  def slower_than(seconds)
    queries.select do |query|
      query['Query_time'] > seconds
    end
  end

  private

  attr_accessor :_query, :_comment

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

  def queries
    reset!
    Enumerator.new do |yielder|
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
end
