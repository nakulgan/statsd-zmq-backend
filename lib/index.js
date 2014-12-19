/*jshint node:true, laxcomma:true */

/*
* Flush stats via ZMQ Parallel Pipeline Push(http://zeromq.org)
* To enable this backend, include 'zmq-push' in the backends
* congiguration array:
*   backends: ['statsd-zmq-backend/lib/index']
*/

var zmq = require('zmq');

var l;
var debug;
var flushInterval;
var zmqHost;
var zmqPort;
var flushCounts;

// prefix configuration
var globalPrefix;
var prefixCounter;
var prefixTimer;
var prefixGauge;
var prefixSet;
var prefixStats;

// set up namespaces
var globalNamespace  = [];
var counterNamespace = [];
var timerNamespace   = [];
var gaugesNamespace  = [];
var setsNamespace    = [];

var zmqStats = {};

var ts_suffix = "";
var starttime = Date.now();
var statString = "";
var numStats = 0;

function prepCounters(metrics){
  var statString = '';
  var numStats = 0;
  var counters = metrics.counters;
  var counter_rates = metrics.counter_rates;
  
  // Counters
  for (var key in counters) {
    var namespace = counterNamespace.concat(key);
    var value = counters[key];
    var valuePerSecond = counter_rates[key];
    statString += namespace.concat('rate').join(".") + " " + valuePerSecond + ts_suffix;
    if (flushCounts) {
      statString += namespace.concat('count').join(".") + " " + value + ts_suffix;
    }
    numStats += 1;
  }
  return [statString, numStats];
};

function prepTimers(metrics){
  var statString = '';
  var numStats = 0;
  var timers = metrics.timers;
  var timer_data = metrics.timer_data;
  // Timers
  for (var key in timer_data) {
    var namespace = timerNamespace.concat(key);
    var the_key = namespace.join(".");
    for (var timer_data_key in timer_data[key]) {
      if (typeof(timer_data[key][timer_data_key]) === 'number') {
        statString += the_key + '.' + timer_data_key  + " " + timer_data[key][timer_data_key] + ts_suffix;
      } else {
        for (var timer_data_sub_key in timer_data[key][timer_data_key]) {
          if (debug) {
            l.log(timer_data[key][timer_data_key][timer_data_sub_key].toString());
          }
          statString += the_key + '.' + timer_data_key + '.' + timer_data_sub_key  + " " +
                        timer_data[key][timer_data_key][timer_data_sub_key] + ts_suffix;
        }
      }
    }
    numStats += 1;
  }
  return [statString, numStats];
}

function prepGauges(metrics){
  var statString = '';
  var numStats = 0;
  var gauges = metrics.gauges;
  // Gauges
  for (var key in gauges) {
    var namespace = gaugesNamespace.concat(key);
    statString += namespace.join(".") + " " + gauges[key] + ts_suffix;
    numStats += 1;
  }
  return [statString, numStats];
};

function prepSets(metrics){
  var statString = '';
  var numStats = 0;
  var sets = metrics.sets;
  // Sets
  for (var key in sets) {
    var namespace = setsNamespace.concat(key);
    statString += namespace.join(".") + '.count' + " " + sets[key].values().length + ts_suffix;
    numStats += 1;
  }
  return [statString, numStats];
};

function zmqPush(startupTime, config, emitter, logger) {
  var self = this;
  l       = logger;
  debug   = config.debug;
  zmqHost = config.zmqHost;
  zmqPort = config.zmqPort;

  // set prefixes based on protocol type
  globalPrefix    = config.zmq.globalPrefix;
  prefixCounter   = config.zmq.prefixCounter;
  prefixTimer     = config.zmq.prefixTimer;
  prefixGauge     = config.zmq.prefixGauge;
  prefixSet       = config.zmq.prefixSet;
  // globalSuffix   = config.zmq.globalSuffix;

  // set defaults for prefixes & suffix
  globalPrefix  = globalPrefix !== undefined ? globalPrefix : "stats";
  prefixCounter = prefixCounter !== undefined ? prefixCounter : "counters";
  prefixTimer   = prefixTimer !== undefined ? prefixTimer : "timers";
  prefixGauge   = prefixGauge !== undefined ? prefixGauge : "gauges";
  prefixSet     = prefixSet !== undefined ? prefixSet : "sets";
  prefixStats   = prefixStats !== undefined ? prefixStats : "statsd";

  // Add global prefix to namespaces
  globalNamespace.push(globalPrefix);
  counterNamespace.push(globalPrefix);
  timerNamespace.push(globalPrefix);
  gaugesNamespace.push(globalPrefix);
  setsNamespace.push(globalPrefix);

  // Add metric speicifc prefix to namespaces
  counterNamespace.push(prefixCounter);
  timerNamespace.push(prefixTimer);
  gaugesNamespace.push(prefixGauge);
  setsNamespace.push(prefixSet);

  // TODO: global suffix

  zmqStats.last_flush = startupTime;
  zmqStats.last_exception = startupTime;
  zmqStats.flush_time = 0;
  zmqStats.flush_length = 0;

  flushInterval = config.flushInterval;
  flushCounts = typeof(config.flush_counts) === "undefined" ? true : config.flush_counts;

  sock = zmq.socket('push')
  sock.bindSync('tcp://'+ config.zmqHost + ':' + config.zmqPort);
  l.log("Bound to: " + config.zmqHost + ":" + config.zmqPort);

  emitter.on('status', function(callback) { status(callback); });
  emitter.on('flush', function(timestamp, metrics) { flushLegacyStringMetrics(timestamp, metrics); });
  return true;
};

var legacyStringMetrics = {
  prepCounters : prepCounters,
  prepTimers : prepTimers,
  prepSets : prepSets,
  prepGauges : prepGauges
};

flushLegacyStringMetrics = function(ts, metrics) {
  ts_suffix = ' ' + ts + "\n";
  starttime = Date.now();
  statString = '';
  numStats = 0;

  // Build String
  // TODO: Async Parallel/Map?
  ['prepCounters', 'prepTimers', 'prepGauges', 'prepSets'].forEach(function(key) {
    prep_key_metrics = legacyStringMetrics[key](metrics);
    statString += prep_key_metrics[0];
    numStats += prep_key_metrics[1];
  });

  // Send
  sock.send(statString);

  if (debug) {
    l.log("numStats: " + numStats);
  }
};

status = function(write) {
    for (var stat in zmqStats) {
    write(null, 'zmqPush', stat, zmqStats[stat]);
  }
};

exports.init = function(startupTime, config, events, logger){
  var initialize = zmqPush(startupTime, config, events, logger);
  if (initialize){
    return true;
  }
}