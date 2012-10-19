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
    
    def mean(arr)
      arr.inject(0) { |a, b| a + b } / arr.size
    end
    
    def test_evaluate_max_of_filter
      # medals := //summer_games/london_medals
      medals = load_file 'summer_games/london_medals.json'

      # medals where medals.Sex = "F"
      medals_prime = medals.select { |m| m['Sex'] = 'F' }

      # max(medals.G where medals.Sex = "F")
      results = medals_G = medals_prime.map { |p| p['G'] }.max

      results_must " haveSize(1)"
      results_must " contain(#{render_value results})"
    end

    def test_evaluate_a_histogram_of_state_on_tweets_union_tweets
      # tweets = //election/tweets
      tweets = load_file 'election/tweets.json'
      
      # tweets.STATE
      tweets_STATE = tweets.map { |p| p['STATE'] }
      
      # solve 'state
      #   tweets' := tweets where tweets.STATE = 'state
      #   { state: 'state, count: count(tweets') }
      results = tweets_STATE.uniq.map do |state|
        { "state" => state, "count" => tweets_STATE.select { |state2| state == state2 }.size }
      end
      
      results_must " haveSize(#{results.size})"
      
      results.uniq.each do |res|
        results_must " contain(#{render_value res})"
      end
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
    
    def test_determine_a_histogram_of_a_composite_key_of_revenue_and_campaign
      # campaigns := //campaigns
      campaigns = load_file 'campaigns.json'
      
      # organizations := //organizations
      organizations = load_file 'organizations.json'
      
      # solve 'revenue, 'campaign
      #   organizations' := organizations where organizations.revenue = 'revenue
      #   campaigns' := campaigns where campaigns.campaign = 'campaign
      #   organizations'' := organizations' where organizations'.campaign = 'campaign
      #   
      #   campaigns' ~ organizations''
      #     { revenue: 'revenue, num: count(campaigns') }
      
      organizations_revenue_campaign = organizations.map do |org|
        [org['revenue'], org['campaign']]
      end.select { |revenue, campaign| revenue && campaign }.uniq
      
      results = organizations_revenue_campaign.map do |revenue, campaign|
        campaigns_bucket = campaigns.select { |c| c['campaign'] == campaign }
        { 'revenue' => revenue, 'num' => campaigns_bucket.size }
      end.select { |obj| obj['num'].to_i > 0 }
      
      results_must " haveSize(#{results.size})"
      
      results.uniq.each do |res|
        obj_body = res.map do |key, value|
          "\"#{key}\" -> #{render_value(value)}"
        end.join ', '
        
        results_must " contain(SObject(Map(#{obj_body})))"
      end
    end
    
    def test_determine_most_isolated_clicks_in_time
      # clicks := //clicks
      clicks = load_file 'clicks.json'
      
      # spacings := solve 'time
      #   click := clicks where clicks.time = 'time
      click_times = clicks.map { |c| c['time'] }.uniq
      
      spacings = click_times.map do |time|
        click = clicks.select { |c| c['time'] == time }
        
        #   belowTime := max(clicks.time where clicks.time < 'time)
        #   aboveTime := min(clicks.time where clicks.time > 'time)
        below_time = click_times.select { |t| t < time }.max
        above_time = click_times.select { |t| t > time }.min
        
        #   {
        #     click: click,
        #     below: click.time - belowTime,
        #     above: aboveTime - click.time
        #   }
        if below_time && above_time && !click.empty?
          click.map do |c|
            {
              'click' => c,
              'below' => time - below_time,
              'above' => above_time - time
            }
          end
        else
          []
        end
      end.flatten
      
      # meanAbove := mean(spacings.above)
      mean_above = mean(spacings.map { |s| s['above'].to_f })
      
      # meanBelow := mean(spacings.below)
      mean_below = mean(spacings.map { |s| s['below'].to_f })

      puts "spacings size: #{spacings.size}"
      puts "mean above: #{mean_above}"
      puts "mean below: #{mean_below}"
      
      # spacings.click where spacings.below < meanBelow | spacings.above > meanAbove
      results = spacings.select do |s|
        s['below'] > mean_below && s['above'] > mean_above
      end.map { |s| s['click'] }
      
      results_must " haveSize(#{results.size})"
      
      results.uniq.each do |res|
        results_must " contain(#{render_value res})"
      end
    end

    def test_determine_surrounding_click_times
      # clicks := //clicks
      clicks = load_file 'clicks.json'
      
      # spacings := solve 'time
      #   click := clicks where clicks.time = 'time
      click_times = clicks.map { |c| c['time'] }.uniq
      
      surrounding = click_times.map do |time|
        click = clicks.select { |c| c['time'] == time }
        
        #   belowTime := max(clicks.time where clicks.time < 'time)
        #   aboveTime := min(clicks.time where clicks.time > 'time)
        below_time = click_times.select { |t| t < time }.max
        above_time = click_times.select { |t| t > time }.min
        
        #   {
        #     click: click,
        #     below: click.time - belowTime,
        #     above: aboveTime - click.time
        #   }
        if below_time && above_time && !click.empty?
          click.map do |c|
            {
              'time' => time,
              'below' => below_time,
              'above' => above_time
            }
          end
        else
          []
        end
      end.flatten
      
      surrounding.each do |res|
        results_must " contain(#{render_value res})"
      end
    end
    
    def test_determine_page0_page1_ratios
      # clicks := //clicks
      clicks = load_file 'clicks.json'
      
      # spacings := solve 'time
      #   click := clicks where clicks.time = 'time
      click_zones = clicks.map { |c| c['timeZone'] }.uniq
      
      ratios = click_zones.map do |tz|
        clicks0 = clicks.select { |c| c['timeZone'] == tz && c['pageId'] == 'page-0' }
        clicks1 = clicks.select { |c| c['timeZone'] == tz && c['pageId'] == 'page-1' }
        
        if (clicks0.size != 0 && clicks1.size != 0) 
          [{
            'timeZone' => tz,
            'ratio' => 100.0 * (clicks0.size.to_f / clicks1.size.to_f)
          }]
        else
          []
        end
      end.flatten
      
      results_must " haveSize(#{ratios.size})"
      ratios.each do |res|
        results_must " contain(#{render_value res})"
      end
    end
    
    def test_perform_a_simple_join_by_value_sorting
      # clicks := //clicks
      clicks = load_file 'clicks.json'
      
      # views := //views
      views = load_file 'views.json'
      
      # clicks ~ views
      #   clicks.time + views.time where clicks.pageId = views.pageId
      results = clicks.map do |click|
        views.map do |view|
          if click['userId'] == view['userId']
            [click['pageId'] + view['pageId']]
          else
            []
          end
        end
      end.flatten
      
      results_must " haveSize(#{results.size})"
      
      results.uniq.each do |result|
        results_must " contain(#{render_value result})"
      end
    end
  end
end

def results_must(assertion)
  puts "  results must#{assertion}"
end

# TODO doesn't handle null
def render_value(value)
  if Hash === value
    body = value.map do |key, value|
      "\"#{key}\" -> #{render_value value}"
    end.join ', '
    
    "SObject(Map(#{body}))"
  elsif Array === value
    body = value.map { |v| render_value v}.join ', '
    
    "SArray(Vector(#{body}))"
  elsif String === value
    "SString(\"#{value}\")"
  elsif Fixnum === value
    "SDecimal(BigDecimal(\"#{value}\"))"
  elsif Float === value
    "SDecimal(BigDecimal(\"#{value}\"))"
  elsif TrueClass === value
    "SBoolean(#{value})"
  elsif FalseClass === value
    "SBoolean(#{value})"
  else
    ''
  end
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
