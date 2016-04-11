# encoding: utf-8
require "securerandom"
require "logstash/filters/base"
require "logstash/namespace"

# The metrics filter is useful for aggregating metrics.
#
# IMPORTANT: Elasticsearch 2.0 no longer allows field names with dots. Version 3.0
# of the metrics filter plugin changes behavior to use nested fields rather than
# dotted notation to avoid colliding with versions of Elasticsearch 2.0+.  Please
# note the changes in the documentation (underscores and sub-fields used).
#
# For example, if you have a field 'response' that is
# a http response code, and you want to count each
# kind of response, you can do this:
# [source,ruby]
#     filter {
#       metrics {
#         meter => [ "http_%{response}" ]
#         add_tag => "metric"
#       }
#     }
#
# Metrics are flushed every 5 seconds by default or according to
# 'flush_interval'. Metrics appear as
# new events in the event stream and go through any filters
# that occur after as well as outputs.
#
# In general, you will want to add a tag to your metrics and have an output
# explicitly look for that tag.
#
# The event that is flushed will include every 'meter' and 'timer'
# metric in the following way:
#
# #### 'meter' values
#
# For a `meter => "something"` you will receive the following fields:
#
# * "[thing][count]" - the total count of events
# * "[thing][rate_1m]" - the 1-minute rate (sliding)
# * "[thing][rate_5m]" - the 5-minute rate (sliding)
# * "[thing][rate_15m]" - the 15-minute rate (sliding)
#
# #### 'timer' values
#
# For a `timer => [ "thing", "%{duration}" ]` you will receive the following fields:
#
# * "[thing][count]" - the total count of events
# * "[thing][rate_1m]" - the 1-minute rate of events (sliding)
# * "[thing][rate_5m]" - the 5-minute rate of events (sliding)
# * "[thing][rate_15m]" - the 15-minute rate of events (sliding)
# * "[thing][min]" - the minimum value seen for this metric
# * "[thing][max]" - the maximum value seen for this metric
# * "[thing][stddev]" - the standard deviation for this metric
# * "[thing][mean]" - the mean for this metric
# * "[thing][pXX]" - the XXth percentile for this metric (see `percentiles`)
#
# #### Example: computing event rate
#
# For a simple example, let's track how many events per second are running
# through logstash:
# [source,ruby]
# ----
#     input {
#       generator {
#         type => "generated"
#       }
#     }
#
#     filter {
#       if [type] == "generated" {
#         metrics {
#           meter => "events"
#           add_tag => "metric"
#         }
#       }
#     }
#
#     output {
#       # only emit events with the 'metric' tag
#       if "metric" in [tags] {
#         stdout {
#           codec => line {
#             format => "rate: %{[events][rate_1m]}"
#           }
#         }
#       }
#     }
# ----
#
# Running the above:
# [source,ruby]
#     % bin/logstash -f example.conf
#     rate: 23721.983566819246
#     rate: 24811.395722536377
#     rate: 25875.892745934525
#     rate: 26836.42375967113
#
# We see the output includes our 'events' 1-minute rate.
#
# In the real world, you would emit this to graphite or another metrics store,
# like so:
# [source,ruby]
#     output {
#       graphite {
#         metrics => [ "events.rate_1m", "%{[events][rate_1m]}" ]
#       }
#     }
class LogStash::Filters::Metrics2 < LogStash::Filters::Base
  config_name "metrics2"

  # syntax: `meter => [ "name of metric", "name of metric" ]`
  config :meter, :validate => :string, :required => true

  # syntax: `timer => [ "name of metric", "%{time_value}" ]`
  config :timer, :validate => :hash, :default => {}

  # Don't track events that have @timestamp older than some number of seconds.
  #
  # This is useful if you want to only include events that are near real-time
  # in your metrics.
  #
  # Example, to only count events that are within 10 seconds of real-time, you
  # would do this:
  #
  #     filter {
  #       metrics {
  #         meter => [ "hits" ]
  #         ignore_older_than => 10
  #       }
  #     }
  config :ignore_older_than, :validate => :number, :default => 0

  # The flush interval, when the metrics event is created. Must be a multiple of 5s.
  config :flush_interval, :validate => :number, :default => 5

  # The clear interval, when all counter are reset.
  #
  # If set to -1, the default value, the metrics will never be cleared.
  # Otherwise, should be a multiple of 5s.
  config :clear_interval, :validate => :number, :default => -1

  # The rates that should be measured, in minutes.
  # Possible values are 1, 5, and 15.
  config :rates, :validate => :array, :default => [1, 5, 15]

  #config :to_emit, :validate => :array, :default => []

  def register
    require "metriks"
    require "socket"
    require "atomic"
    require "thread_safe"
    @random_key_preffix = SecureRandom.hex
    unless (@rates - [1, 5, 15]).empty?
      raise LogStash::ConfigurationError, "Invalid rates configuration. possible rates are 1, 5, 15. Rates: #{rates}."
    end
    @metric_meters = ThreadSafe::Hash.new { |h,k| h[k] = { 'metric' => Metriks.meter(metric_key(k)), 'last_flush' => Atomic.new(0), 'last_clear' => Atomic.new(0) }}
    @to_emit = ThreadSafe::Array.new
  end # def register

  def filter(event)


    # TODO(piavlo): This should probably be moved to base filter class.
    if @ignore_older_than > 0 && Time.now - event.timestamp.time > @ignore_older_than
      @logger.debug("Skipping metriks for old event", :event => event)
      return
    end

    @metric_meters[event.sprintf(@meter)]['metric'].mark


    #@metric_meters.each_pair do |n, m|
    #  @logger.debug('BBBBBBBBBBBBBBBBBBBBB')
    #  @logger.debug(m['last_clear'].value)
    #  if m['last_clear'].value >= 0 and m['last_clear'].value < 10
    #    e = LogStash::Event.new
    #    e['aaa'] = 123
    #    @logger.debug('CCCCCCCCCCCCCCCCCCCCC')
    #    yield e
    #    @logger.debug('DDDDDDDDDDDDDDDDDDDDDDDD')
    #  end
    #end

    #@to_emit.each do |e|
    #  @logger.debug('EEEEEEEEEEEEEEEEE')
    #  yield @to_emit.delete(e) 
    #  #yield e
    #end

  end # def filter

  def flush(options = {})
    # no cancelled
    #return
    events = []
    @metric_meters.each_pair do |name, meter|
      @logger.debug("ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ")
      meter['last_flush'].update { |v| v + 5 }
      meter['last_clear'].update { |v| v + 5 }
      @logger.debug(meter['last_clear'].value)

      # no cancelled
      #return

      next unless should_flush?(meter)

      event = LogStash::Event.new
      event["message"] = "metric"
      event["host"] = Socket.gethostname
      event["name"] = name
      event["count"] = meter['metric'].count
      # no cancelled
      #return

      meter['metric'].clear if should_clear?(meter)

      # Reset counter since metrics were flushed
      meter['last_flush'].value = 0
      #@to_emit.push(event)

      # no cancelled
      #return

      if should_clear?(meter)
        #Reset counter since metrics were cleared
        meter['last_clear'].value = 0
      end
      @logger.debug("XXXXXXXXXXXXXXXXXXXXXXX")

      # no cancelled
      #return

      #yield event

      events.push(event)
      filter_matched(event)

    end
    return events
    #return
  end

  # this is a temporary fix to enable periodic flushes without using the plugin config:
  #   config :periodic_flush, :validate => :boolean, :default => true
  # because this is not optional here and should not be configurable.
  # this is until we refactor the periodic_flush mechanism per
  # https://github.com/elasticsearch/logstash/issues/1839
  def periodic_flush
    true
  end

  private

  def metric_key(key)
    "#{@random_key_preffix}_#{key}"
  end

  def should_flush?(meter)
    meter['last_flush'].value >= @flush_interval
  end

  def should_clear?(meter)
    @clear_interval > 0 && meter['last_clear'].value >= @clear_interval
  end
end # class LogStash::Filters::Metrics
