#!/usr/bin/ruby

require 'rubygems'
require 'json'

Event = Struct.new :num, :id, :path, :token, :data

EVENT_REGEXP = /^Event-([^\s]+) Id: ([^\s]+) Path: ([^\s]+) Token: ([^\s]+)/

def usage
	puts "usage: parse-kafka.rb <event-file.txt> <output-dir>"
	exit 100
end

usage if ARGV.size != 2

input = ARGV[0]
dir = ARGV[1]

def rec_mkdir(dir)
	parent = File.dirname dir
	rec_mkdir parent unless File.exists? parent
	Dir.mkdir dir
end

File.open input, 'r' do |file|
	line = file.gets
	
	while line
		md = EVENT_REGEXP.match line
		
		if md
			num = md[1].to_i
			id = md[2]
			path = md[3]
			token = md[4]
			
			to_parse = ''
			line = file.gets
			while line && !(EVENT_REGEXP.match line)
				to_parse += line
				line = file.gets
			end
			
			begin
				data = JSON.parse to_parse.strip
			rescue Exception => e
				puts e
				puts 'WARNING: failed to process!'
				puts 'Event num: ' + num.to_s
				puts to_parse.strip
			end
			
			if data
				event = Event.new(num, id, path, token, data)
				out_path = (dir + event.path).gsub /\/$/, '.jsonb'
				
				parent_dir = File.dirname out_path
				rec_mkdir parent_dir unless File.exists? parent_dir
				
				File.open out_path, 'a+' do |out_file|
					out_file.puts(JSON.generate event.data)
				end
			end
		else
			puts 'Malformed line: ' + line
			exit -1
		end
	end
end
