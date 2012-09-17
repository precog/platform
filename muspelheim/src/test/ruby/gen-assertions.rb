#!/usr/bin/ruby

require 'rubygems'

require 'json'

DATA_PATH = "#{File.dirname __FILE__}/../resources/test_data/"

module Tests
  class << self
    def load_file(file)
      back = ''
      File.open(DATA_PATH + file, 'r') do |file|
        file.each_line { |line| back += line }
      end
      JSON.parse back
    end
    
    def test_evaluate_a_solve_constrained_by_inclusion
      # clicks = //clicks
      clicks = load_file 'clicks.json'
      
      # views = //views
      views = load_file 'views.json'
      
      # clicks.pageId
      clicks_pageId = clicks.map { |p| p['pageId'] }
      
      # views.pageId
      views_pageId = views.map { |v| v['pageId'] }
      
      # solve 'page = views.pageId
      #   count(clicks where clicks.pageId = 'page)
      filtered = clicks_pageId.select { |id| views_pageId.include? id }
      
      results = filtered.uniq.map do |id|
        clicks_pageId.select { |id2| id == id2 }.size
      end
      
      results_must " haveSize(#{results.size})"
      
      results.uniq.each do |res|
        results_must " contain(#{res})"
      end
    end
  end
end

def results_must(assertion)
  puts "  results must#{assertion}"
end

Tests.methods.each do |sym|
  test_name = sym.to_s.gsub '_', ' '
  md = test_name.match /^test (.+)$/
  
  if md
    puts md[1]
    Tests.method(sym).call
    puts
  end
end
