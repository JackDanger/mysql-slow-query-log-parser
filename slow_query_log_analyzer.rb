# Usage:
#
#   queries = SlowLogAnalyzer.new('/data/fusionio/mysql/slow-query.log')
#
#   queries.slower_than(2.5).reduce({}) do |data, q|
#     source = q[:source_location]
#     h[source] || = 0
#     h[source] += 1
#     h
#   end.sort_by(&:last).reverse.take(10)
#
class SlowLogAnalyzer
  ParserStates = [:loading, :query, :comment]

  def initialize(filename)
    @filename = filename
    @state = :loading
  end

  def slower_than(seconds)
    sections.each do |section|
      yield if section['Query_time'] > seconds
    end
  end

  private

  attr_accessor :query, :line

  def reset!
    @query, @line = [], []
  end

  def build_section(query, comment)
    comment.reduce({sql: query}) do |data, line|
      data.update extract_comment(line)
    end
  end

  def extract_comment(line)
    Hash[
      *line.split(/(\w+: )/).drop(1).map(&:strip).map do |value|
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

  def sections
    reset!

    file.each_line do |l|
      line = l.chomp

      ## This is a shit parser.

      case @state
      # God knows what's at the start of the file.
      when :loading
        if line =~ /^SET /
          @state = :comment
        end
      # We're collecting the lines of a query
      when :query
        if line =~ /^# /
          # we've finished parsing the query
          @state = :comment
          comment << line
        elsif line =~ /^SET /
          # don't care about these statements
        else
          # keep parsing the query
          query << line
        end
      # We're collecting the lines of data on a query
      when :comment
        # keep going?
        if line =~ /^# /
          comment << line
        # finished a comment block?
        else
          yield build_section(query, comment)
          reset!
          @state = :query
          query << line if line =~ /^SET /
        end
      end
    end
  end

  def file
    @file ||= File.open(@filename)
  end
end
