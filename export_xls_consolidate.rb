#!/usr/bin/env ruby
# frozen_string_literal: true

# Bepress Digital Commons' Excel Export creates 2 .xls files per collection.
# It may output the files for many collections in the folder for a higher-level collection.
# The zipped up export package received from a client will usually contain multiple
#   folders, each containing the files for one or more collections.
# As it is not feasible to get an overview of the data we are dealing with by looking at
#   so many separate files, this script can be used to compile one big general metadata
#   report and one big editor report

# standard library
require 'csv'
require 'optparse'
require 'pp'

require 'bundler/inline'

gemfile do
  source 'https://rubygems.org'
  gem 'pry'
  gem 'roo-xls'
end

require 'spreadsheet'

@options = {}
OptionParser.new do |opts|
  opts.banner = 'Usage: xls_stats.rb -i path-to-input-dir -o path-to-output-directory'

  opts.on('-i', '--input PATH', 'Path to directory containing the directories that contain .xls files') do |i|
    i = File.expand_path(i)
    if Dir::exist?(i)
      @options[:input] = i
    else
      puts "Not a valid input directory: #{i}"
      exit
    end
  end

  opts.on('-o', '--output PATH', 'Path to output directory') do |o|
    @options[:output] = File.expand_path(o)
    Dir::mkdir(o) unless Dir::exist?(o)
  end

  opts.on('-h', '--help', 'Prints this help') do
    puts opts
    exit
  end
end.parse!

class CollDir
  attr_reader :name, :path
  def initialize(name:, parent:)
    @name = name
    @path = "#{parent}/#{name}"
  end
  
  # returns array of collection names for which there are files in the directory
  def colls
    ed_reports.map{ |r| r.sub('_editor_report.xls', '') }
  end

  # returns path to editor report for the collection given
  def ed_report_for(coll)
    arr = ed_reports.select{ |fn| fn.start_with?("#{coll}_editor_report") }
    return nil if arr.empty?
    "#{path}/#{arr.first}"
  end
  
  # returns array of editor report filenames
  def ed_reports
    Dir.children(path).select{ |fn| fn.end_with?('editor_report.xls') }
  end

  # returns array of paths to main reports for the collection given
  def main_reports_for(coll)
    arr = main_reports.select{ |fn| fn.match?(/^#{coll}_\d+\.xls/) }
    return nil if arr.empty?
    arr.map{ |e| "#{path}/#{e}" }
  end

  # returns array of main report filenames
  def main_reports
    Dir.children(path).reject{ |fn| fn.end_with?('editor_report.xls') }
  end
end

class Collection
  attr_reader :name, :parent, :fullname, :ed_report, :main_reports
  def initialize(name:, dir:)
    @name = name
    @parent = dir.name
    @fullname = "#{parent}/#{name}"
    @ed_report = CollFile.new(path: dir.ed_report_for(name), coll: name)
    @main_reports = dir.main_reports_for(name).map{ |path| CollFile.new(path: path, coll: fullname) }
    mismatched_headers_warning unless multi_file_headers_match?
  end

  # returns array of headers for editor report
  def ed_headers
    ed_report.headers
  end
  # true if only editor report populated
  def ed_only?
    !empty_ed? && empty_main? ? true : false
  end
  
  # true if all associated reports are empty
  def empty?
    empty_ed? && empty_main? ? true : false
  end

  # true if editor report is empty
  def empty_ed?
    ed_report.empty? ? true : false
  end

  # true if main reports are empty
  def empty_main?
    result = main_reports.map{ |r| r.empty? }.uniq
    result.length == 1 && result.first == true ? true : false
  end

  # true if neither associated report is empty
  def full?
    !empty_ed? && !empty_main? ? true : false
  end

  # returns array of headers for editor report
  def main_headers
    main_reports.map(&:headers).flatten.uniq
  end

  # true if only main reports populated
  def main_only?
    empty_ed? && !empty_main? ? true : false
  end
  
  # is there more than one main file?
  def multi_main?
    main_reports.length > 1 ? true : false
  end
  
  private

  def mismatched_headers_warning
    puts "WARNING: Headers differ in the multiple main reports for #{fullname}. The script assumes headers in multiple files for the same collection will be identical"
  end
  
  def multi_file_headers_match?
    return true if main_reports.length == 1
    
    header_sets = main_reports.map{ |r| r.headers }
    first = header_sets.shift
    results = header_sets.map{ |arr| arr.length == first.length && ( arr - first ).length == 0 }
    results.uniq!
    results.length == 1 && results.first == true ? true : false
  end
end

class CollFile
  attr_reader :path, :coll
  def initialize(path:, coll:)
    @path = path
    @coll = coll
  end

  def column_count
    headers.length
  end

  # returns array of header values
  def headers
    @header_data = get_headers if @header_data.nil?
    @header_data
  end

  # returns true if there are no rows besides header
  def empty?
    row_count == 0 ? true : false
  end

  # returns integer count of data rows (headers not counted)
  def row_count
    @row_count = get_row_count if @row_count.nil?
    @row_count
  end
  
  # returns array of rows, where each row is a hash
  # drops first row (headers)
  def rows
    @rows = get_rows if @rows.nil?
    @rows
  end
  
  private
  
  def get_headers
    spreadsheet.row(1)
  end

  def get_row_count
    spreadsheet.last_row - 1
  end

  # returns array of rows, where each row is a hash
  # drops first row (headers)
  def get_rows
    data = spreadsheet.parse(headers: true)
    data.shift
    data
  end

  def spreadsheet
    Roo::Spreadsheet.open(path).sheet(0)
  end
end

class CollectionsHolder
  attr_reader :all
  def initialize(dir:)
    children = Dir.children(dir)
    puts "Processing #{children.length} directories..."
    dirs = children.map{ |d| CollDir.new(name: d, parent: dir) }
    collsets = dirs.map{ |dir| dir.colls }
    puts "...containing #{collsets.flatten.length} collections"
    @all = dirs.map{ |dir| dir.colls.map{ |coll| Collection.new(name: coll, dir: dir) } }.flatten!
  end
  
  def ed_header_hash
    @ed_header_data = get_ed_header_hash if @ed_header_data.nil?
    @ed_header_data
  end

  def ed_headers
    with_ed.map{ |c| c.ed_headers }.flatten.uniq
  end
  
  def empty
    @empty_colls = get_empty if @empty_colls.nil?
    @empty_colls
  end

  def full
    @full_colls = get_full if @full_colls.nil?
    @full_colls
  end

  def ed_only
    @ed_only_colls = get_ed_only if @ed_only_colls.nil?
    @ed_only_colls
  end

  def main_header_hash
    @main_header_data = get_main_header_hash if @main_header_data.nil?
    @main_header_data
  end

  def main_headers
    with_main.map{ |c| c.main_headers }.flatten.uniq
  end

  def main_only
    @main_only_colls = get_main_only if @main_only_colls.nil?
    @main_only_colls
  end

  def with_ed
    full + ed_only
  end
  
  def with_main
    full + main_only
  end

  private
  
  # returns hash of editor header data for all collections
  #  that are not empty and have populated editor report
  def get_ed_header_hash
    selection = all - empty - main_only
    selection.map{ |c| [c.fullname, c.ed_headers] }.to_h
  end
  
  def get_empty
    all.select{ |c| c.empty? }
  end

  def get_full
    all.select{ |c| c.full? }
  end

  # returns hash of main header data for all collections
  #  that are not empty and have populated main report
  def get_main_header_hash
    selection = all - empty - ed_only
    selection.map{ |c| [c.fullname, c.main_headers] }.to_h
  end

  def get_partial
    all - empty - full
  end
  
  def get_ed_only
    get_partial.select{ |c| c.ed_only? }
  end

  def get_main_only
    get_partial.select{ |c| c.main_only? }
  end
end

# data = CollectionsHolder.*_header_hash
# path = to write report
class HeaderReporter
  COLNAME = 'column_name'
  COLCT = 'collection_count'
  
  def initialize(data:, path:)
    @data = data
    @path = path
  end

  def uniq_headers
    @data.values.flatten.uniq
  end

  def report_headers
    [COLNAME, COLCT, colls].flatten
  end

  def compiled
    @compiled_data = get_compiled if @compiled_data.nil?
    @compiled_data
  end

  def write
    CSV.open(path, 'wb', headers: true) do |csv|
      csv << report_headers
      rows.each{ |r| csv << r.values_at(*report_headers) }
    end
  end
  
  private
  
  def get_compiled
    h = uniq_headers.map{ |hdr| [hdr, []] }.to_h
    @data.each do |coll, hdrs|
      hdrs.each{ |hdr| h[hdr] << coll }
    end
    h
  end

  def colls
    @data.keys
  end

  # prepares array of row-hashes for writing to csv
  def rows
    uniq_headers.map{ |hdr| row(hdr) }
  end

  # header is the name of a column header in the data sheets
  def row(header)
    r = { COLNAME => header }
    colls_with_header = compiled[header]
    r[COLCT] = colls_with_header.length
    colls.each{ |coll| r[coll] = colls_with_header.any?(coll) ? 1 : 0 }
    r
  end
end

class CompiledReporter
  COLLNAME = 'dc_collection_name'
  UNUSED = '%FIELD NOT USED IN COLLECTION%'
  attr_reader :path, :headers
  def initialize(path:, headers:)
    @path = path
    @headers = headers.unshift(COLLNAME)
    CSV.open(path, 'wb', headers: true){ |csv| csv << headers }
  end

  def append_file(coll:, data:)
    CSV.open(path, 'a', headers: true) do |csv|
      prep_data(coll, data).each do |row|
        csv << row.values_at(*headers)
      end
    end
    puts "  #{coll} data written"
  end

  private

  def headers_in_data(data)
    data.first.keys
  end

  def missing_headers(data)
    headers - headers_in_data(data)
  end
  
  def prep_data(coll, data)
    data = data.each{ |row| row[COLLNAME] = coll }
    missing = missing_headers(data)
    data = add_missing(missing, data) unless missing.empty?
    data
  end

  def add_missing(missing_headers, data)
    data = data.each do |row|
      missing_headers.each{ |hdr| row[hdr] = UNUSED }
    end
    data
  end
end

colls = CollectionsHolder.new(dir: @options[:input])

puts "Empty collections: #{colls.empty.length}"
puts "Populated collections: #{colls.full.length}"
puts "Ed only collections: #{colls.ed_only.length}"
puts "Main only collections: #{colls.main_only.length}"

puts "\nCompiling report of headings in editor reports"
hr = HeaderReporter.new(data: colls.ed_header_hash, path: "#{@options[:output]}/columns_editor.csv")
hr.write

puts "\nCompiling report of headings in main reports"
hr = HeaderReporter.new(data: colls.main_header_hash, path: "#{@options[:output]}/columns_main.csv")
hr.write

puts "\nCompiling data from main reports"
cr = CompiledReporter.new(path: "#{@options[:output]}/compiled_main.csv", headers: colls.main_headers)
colls.with_main.each do |coll|
  coll.main_reports.each do |mainrpt|
    cr.append_file(coll: coll.fullname, data: mainrpt.rows)
  end
end

puts "\nCompiling data from ed reports"
cr = CompiledReporter.new(path: "#{@options[:output]}/compiled_ed.csv", headers: colls.ed_headers)
colls.with_ed.each do |coll|
    cr.append_file(coll: coll.fullname, data: coll.ed_report.rows)
end
