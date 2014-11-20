var
  r = require('rethinkdb'),
  async = require('async'),
  util = require('util'),
  _ = require('underscore');

module.exports = RethinkDBProvider;

function RethinkDBProvider(options, helpers, callback) {
  if (!(this instanceof RethinkDBProvider)) return new RethinkDBProvider(options);

  callback = callback || function () {
  };

  var self = this;

  this.name = 'RethinkDB';
  this.options = options;
  this.logger = helpers.logger;
  this.common = helpers.common;
  this.memory = helpers.memory;

  return this.init(options, function (err) {
    if (err)
      return callback(err);

    return callback(null, self);
  });
}

RethinkDBProvider.prototype.init = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  self.logger.info('Initializing connection to provider [' + self.name + '].');

  return self.openConnection(options, callback);
};

RethinkDBProvider.prototype.destroy = function (callback) {
  callback = callback || function () {
  };

  var self = this;

  self.logger.info('Destroying connection to provider [' + self.name + '].');

  return self.closeConnection(options, callback);
};

RethinkDBProvider.prototype.find = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

RethinkDBProvider.prototype.delete = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

RethinkDBProvider.prototype.update = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;

  return callback(null);
};

RethinkDBProvider.prototype.insert = function (collection, documents, options, callback) {
  callback = callback || function () {
  };
  var self = this;
  async.map(documents, function (doc, callback) {
    //doc.timestamp = (doc.timestamp ? new Date(doc.timestamp) : null) || new Date();
    doc.saved = true;
    return callback(null);
  }, function (err, results) {
    if (self.memory.get('rdb:table:' + collection.storeKey)) {
      r.table(collection.storeKey).insert(documents).run(self.client, function (err, result) {
        if (err)
          return callback(err);
        return callback(null, documents, result);
      });
    }
    else {
      r.db(self.options.db).tableCreate(collection.storeKey).run(self.client, function (err, result) {
        //ignore errors if table already exists
        r.db(self.options.db).table(collection.storeKey).indexCreate('timestamp').run(self.client, function (err, result) {
          //ignore errors if index already exists
          self.memory.set('rdb:table:' + collection.storeKey, 1);
          r.table(collection.storeKey).insert(documents).run(self.client, function (err, result) {
            if (err)
              return callback(err);
            return callback(null, documents, result);
          });
        });
      });
    }
  })
};

RethinkDBProvider.prototype.buildQueryPlan = function (query, callback) {
  var self = this;
  var plan = {
    uid: self.common.uuid(),
    cost: 0,
    colQueries: {},
    query: query
  };
  var $match = {};
  var $project = {};
  var $group = {};
  var $map = {};
  var $reduce = {};
  var $final = {};
  var $limit;
  var $sort = {};

  if (!query.dimensions)
    query.dimensions = [];
  if (!query.metrics)
    query.metrics = [];

  if (query.timeframe && !query.timeframe.hasOwnProperty('last_n_items')) {
    if (typeof query.timeframe.start === 'string')
      query.timeframe.start = new Date(query.timeframe.start);
    if (typeof query.timeframe.end === 'string')
      query.timeframe.end = new Date(query.timeframe.end);
    $match.timestamp = {$gt: 'r.ISO8601("' + query.timeframe.start.toISOString() + '")', $lt: 'r.ISO8601("' + query.timeframe.end.toISOString() + '")'};
  }
  else if (query.timeframe && query.timeframe.hasOwnProperty('last_n_items')) {
    $limit = query.timeframe.last_n_items;
    $sort = {timestamp: -1};
  }

  if (query.filter) {
    query.filter.forEach(function (f) {
      //if (f[1] == 'eq')
      //  $match[f[0]] = f[2];
      //else {
      $match[f[0]] = {};
      $match[f[0]]['$' + f[1]] = f[2];
      //}
    });
  }
  if (query.sort) {
    query.sort.forEach(function (s) {
      $sort[s[0]] = {};
      $sort[s[0]] = s[1];
    });
  }

  switch (query.interval) {
    case 'timebucket.second':
      query.interval = 'r.time(doc("timestamp").year(), doc("timestamp").month(), doc("timestamp").day(), doc("timestamp").hours(), doc("timestamp").minutes(), doc("timestamp").seconds(), "Z")';
      break;
    case 'timebucket.minute':
      query.interval = 'r.time(doc("timestamp").year(), doc("timestamp").month(), doc("timestamp").day(), doc("timestamp").hours(), doc("timestamp").minutes(), 0, "Z")';
      break;
    case 'timebucket.hour':
      query.interval = 'r.time(doc("timestamp").year(), doc("timestamp").month(), doc("timestamp").day(), doc("timestamp").hours(), 0, 0, "Z")';
      break;
    case 'timebucket.ddate':
      query.interval = 'r.time(doc("timestamp").year(), doc("timestamp").month(), doc("timestamp").day(), "Z")';
      break;
    case 'timebucket.month':
      query.interval = 'r.time(doc("timestamp").year(), doc("timestamp").month(), 1, "Z")';
      break;
    case 'timebucket.year':
      query.interval = 'r.time(doc("timestamp").year(), 1, 1, "Z")';
      break;
    default:
      break;
  }
  query.dimensions.forEach(function (dimension) {
    switch (dimension.datatype) {
      case 'date':
        //$project.timestamp = 'timestamp';
        //$final.timestamp = 'timestamp(1' + query.interval + ')';
        query.timeSeries = true;

        $final[dimension.key] = dimension.key + ': doc("reduction")("' + dimension.key + '")';
        $group[dimension.key] = -1;
        break;
      case 'ip':
      case 'number':
      case 'string':
        $final[dimension.key] = dimension.key + ': doc("reduction")("' + dimension.key + '")';
        $group[dimension.key] = -1;
        break;
      case 'geo':
        break;
      default:
        return setImmediate(function () {
          return callback(new Error('Dimension [' + dimension.key + '] has unknown type of [' + dimension.datatype + ']'));
        });
    }
  });

  if (query.metrics.length === 0) {
    try {
      query.metrics.push({
        key: 'fake',
        dependsOn: 'fake',
        collection: query.collection.key || query.dimensions ? query.dimensions[0].collection : null
      });
    }
    catch (ex) {
      query.metrics = [];
    }
  }

  query.metrics.forEach(function (metric) {
    var colQuery = {
      table: null,
      query: [],
      pipeline: []
    };
    if (metric.collection) {
      if (typeof metric.collection === 'object') {
        if (metric.collection.storeKey)
          colQuery.table = metric.collection.storeKey;
        else
          colQuery.table = metric.collection.key;
      }
      else
        colQuery.table = metric.collection;
    }
    if (colQuery.table)
      colQuery.table = colQuery.table.replace(/[^\w\s]/gi, '');

    var _$match = self.common.extend({}, $match);
    var _$project = self.common.extend({}, $project);
    var _$group = self.common.extend({}, $group);
    var _$map = self.common.extend({}, $map);
    var _$reduce = self.common.extend({}, $reduce);
    var _$final = self.common.extend({}, $final);

    if (metric.filter) {
      metric.filter.forEach(function (f) {
        // if (f[1] === 'eq')
        //   _$match[f[0]] = f[2];
        //else {
        _$match[f[0]] = {};
        _$match[f[0]]['$' + f[1]] = f[2];
        // }
      });
    }

    var pipelinetype = 'dimensions+metrics';
    if (query.dimensions.length === 0)
      pipelinetype = 'metrics'
    if (query.metrics.length === 0)
      pipelinetype = 'dimensions'

    if (!metric.formula && metric.collection) {
      metric.aggregation = metric.aggregation || 'sum';
      if (metric.aggregation == 'ucount')
        colQuery.type = 'ucount';
      else
        colQuery.type = 'plain';

      colQuery.key = self.common.hash(colQuery.type + '_' + metric.key + '_', metric.collection.key + '_' + JSON.stringify(_$match));
      if (plan.colQueries[colQuery.key]) {
        _$map = self.common.extend({}, plan.colQueries[colQuery.key].query.$map);
        _$map = self.common.extend(plan.colQueries[colQuery.key].query.$map, _$map);
        _$reduce = self.common.extend({}, plan.colQueries[colQuery.key].query.$reduce);
        _$reduce = self.common.extend(plan.colQueries[colQuery.key].query.$reduce, _$reduce);
        _$final = self.common.extend({}, plan.colQueries[colQuery.key].query.$final);
        _$final = self.common.extend(plan.colQueries[colQuery.key].query.$final, _$final);
      }

      //let's iterate over the dimensions again and make sure the table holds them
      Object.keys(_$group).forEach(function (grouped) {
        var exist = _.find(metric.collection.dimensions, function (dimension) {
          return dimension.key.replace(/\./, '_') === grouped;
        });
        if (!exist) {
          //this dimension is missing from the table, let's fake its map-reduce
          _$group[grouped] = 0;
          _$map[grouped] = grouped + ': null';
          _$reduce[grouped] = grouped + ': null';
        }
        else {
          //this is a real dimension and we can map reduce it easily.
          _$group[grouped] = 1;
          if (grouped === 'timestamp')
            _$map[grouped] = grouped + ': ' + query.interval + '';
          else
            _$map[grouped] = grouped + ': doc("' + grouped.replace('_', '")("') + '")';
          _$reduce[grouped] = grouped + ': left("' + grouped + '")';
        }
      });

      if (metric.key !== 'fake') {
        metric.aggregation = metric.aggregation || 'sum';
        if (Array.isArray(metric.dependsOn))
          metric.dependsOn = metric.dependsOn[0];
        if (typeof metric.attribute !== 'string')
          metric.attribute = metric.dependsOn || metric.key;

        metric.attribute = metric.attribute.replace(/\./ig, '")("');
        metric._dependsOn = metric.dependsOn.replace(/\./ig, '_');

        switch (metric.aggregation) {
          case 'avg':
            if (Object.keys($group).length > 0) {
              _$map['sum_' + metric._dependsOn] = 'sum_' + metric._dependsOn + ': doc("' + metric.attribute + '")';
              _$map['count_' + metric._dependsOn] = 'count_' + metric._dependsOn + ': 1';
              _$reduce['sum_' + metric._dependsOn] = 'sum_' + metric._dependsOn + ': left("sum_' + metric._dependsOn + '").add(right("sum_' + metric._dependsOn + '"))';
              _$reduce['count_' + metric._dependsOn] = 'count_' + metric._dependsOn + ': left("count_' + metric._dependsOn + '").add(right("count_' + metric._dependsOn + '"))';
              _$final[metric.key] = metric.key + ': doc("reduction")("sum_' + metric._dependsOn + '").div(doc("reduction")("count_' + metric._dependsOn + '"))';
            }
            else {
              _$map['sum_' + metric._dependsOn] = 'sum_' + metric._dependsOn + ': doc("' + metric.attribute + '")';
              _$map['count_' + metric._dependsOn] = 'count_' + metric._dependsOn + ': 1';
              _$reduce['sum_' + metric._dependsOn] = 'sum_' + metric._dependsOn + ': left("sum_' + metric._dependsOn + '").add(right("sum_' + metric._dependsOn + '"))';
              _$reduce['count_' + metric._dependsOn] = 'count_' + metric._dependsOn + ': left("count_' + metric._dependsOn + '").add(right("count_' + metric._dependsOn + '"))';
              _$final[metric.key] = metric.key + ': doc("sum_' + metric._dependsOn + '").div(doc("count_' + metric._dependsOn + '"))';
            }
            break;
          case 'distinct':
          case 'unique':
          case 'ucount':
            _$map[metric._dependsOn] = metric._dependsOn + ': doc("' + metric.attribute + '")';
            _$group[metric._dependsOn] = _$group[metric._dependsOn] || 2;
            _$reduce['count_' + metric._dependsOn] = 'count_' + metric._dependsOn + ': left("' + 'count_' + metric._dependsOn + '").add(right("' + 'count_' + metric._dependsOn + '"))';
            _$final[metric.key] = metric.key + ': doc("reduction")("count_' + metric._dependsOn + '")';
            break;
          case 'max':
            if (Object.keys($group).length > 0) {
              _$map[metric.aggregation + '_' + metric._dependsOn] = metric.aggregation + '_' + metric._dependsOn + ': doc("' + metric.attribute + '")';
              _$reduce[metric.aggregation + '_' + metric._dependsOn] = metric.aggregation + '_' + metric._dependsOn + ': r.branch(left("' + metric.aggregation + '_' + metric._dependsOn + '").gt(right("' + metric.aggregation + '_' + metric._dependsOn + '")), left("' + metric.aggregation + '_' + metric._dependsOn + '"), right("' + metric.aggregation + '_' + metric._dependsOn + '"))';
            }
            else {
              _$map[metric.key] = metric.key + ': doc("' + metric.attribute + '")';
              _$reduce[metric.key] = metric.key + ': r.branch(left("' + metric.key + '").gt(right("' + metric.key + '")), left("' + metric.key + '"), right("' + metric.key + '"))';
            }
            _$final[metric.key] = metric.key + ': doc("reduction")("' + metric.aggregation + '_' + metric.attribute + '")';
            break;
          case 'min':
            if (Object.keys($group).length > 0) {
              _$map[metric.aggregation + '_' + metric._dependsOn] = metric.aggregation + '_' + metric._dependsOn + ': doc("' + metric.attribute + '")';
              _$reduce[metric.aggregation + '_' + metric._dependsOn] = metric.aggregation + '_' + metric._dependsOn + ': r.branch(left("' + metric.aggregation + '_' + metric._dependsOn + '").lt(right("' + metric.aggregation + '_' + metric._dependsOn + '")), left("' + metric.aggregation + '_' + metric._dependsOn + '"), right("' + metric.aggregation + '_' + metric._dependsOn + '"))';
            }
            else {
              _$map[metric.key] = metric.key + ': doc("' + metric.attribute + '")';
              _$reduce[metric.key] = metric.key + ': r.branch(left("' + metric.key + '").lt(right("' + metric.key + '")), left("' + metric.key + '"), right("' + metric.key + '"))';
            }
            _$final[metric.key] = metric.key + ': doc("reduction")("' + metric.aggregation + '_' + metric.attribute + '")';
            break;
          case 'sum':
          default:
            if (Object.keys($group).length > 0) {
              _$map[metric.aggregation + '_' + metric._dependsOn] = metric.aggregation + '_' + metric._dependsOn + ': doc("' + metric.attribute + '")';
              _$reduce[metric.aggregation + '_' + metric._dependsOn] = metric.aggregation + '_' + metric._dependsOn + ': left("' + metric.aggregation + '_' + metric._dependsOn + '").add(right("' + metric.aggregation + '_' + metric._dependsOn + '"))';
            }
            else {
              _$map[metric._dependsOn] = metric._dependsOn + ': doc("' + metric.attribute + '")';
              _$reduce[metric._dependsOn] = metric._dependsOn + ': left("' + metric._dependsOn + '").add(right("' + metric._dependsOn + '"))';
            }
            _$final[metric.key] = metric.key + ': doc("reduction")("' + metric.aggregation + '_' + metric._dependsOn + '")';
            break;
        }
      }
      colQuery.query = {
        $match: _$match,
        $sort: {timestamp: -1},
        $group: _$group,
        $map: _$map,
        $reduce: _$reduce,
        $final: _$final
      };
      if ($limit)
        colQuery.query.$limit = $limit;
      if ($sort)
        colQuery.query.$sort = $sort;

      plan.colQueries[colQuery.key] = colQuery;
    }
  });

  //console.log('plan', require('util').inspect(plan.colQueries, {depth: null, colors: true}));

  plan.dimensions = query.dimensions;
  plan.metrics = query.metrics;

  //console.log(plan);

  return setImmediate(function () {
    return callback(null, plan);
  });
};


RethinkDBProvider.prototype.query = function (context, query, callback) {
  callback = callback || function () {
  };

  var self = this;

  return self.buildQueryPlan(query, function (err, plan) {
    if (err)
      return callback(err);
    //console.log(require('util').inspect(plan.colQueries, {depth: null, colors: true}));

    var calls = [];
    var results = [];
    Object.keys(plan.colQueries).forEach(function (key) {
      var _plan = plan.colQueries[key];
      var queryPlan = plan.colQueries[key].query;
      var queryPlanKey = key;

      var call = function (callback) {
        var cb = function (err, cursor) {
          if (err) {
            if (err.toString().indexOf('does not exist') === -1)
              return callback(err);
            else
              return callback(null, []);
          }
          if (typeof cursor === 'object') {
            results.push(Array.isArray(cursor) ? cursor : [cursor]);
            return callback(null, cursor);
          }
          else {
            cursor.toArray(function (err, result) {
              if (err) {
                return callback(err);
              }
              else {
                //console.log(require('util').inspect(result, {depth: null, colors: true}));
                results.push(result)
                return callback(null, result);
              }
            });
          }
        };

        var sort = '';
        var group = '';
        var filter = '';
        var map = '';
        var map2 = '';
        var group2 = '';
        var final = '';
        var reduce = '';
        var statement = '';

        //filter = 'function filter (doc){';
        //filter += 'return {';
        Object.keys(queryPlan.$match).forEach(function (key) {
          filter += '.filter(r.row("' + key + '")';
          var first_condition = '';
          if (Object.keys(queryPlan.$match[key]).length == 2) {
            first_condition = Object.keys(queryPlan.$match[key])[0];
            var second_condition = Object.keys(queryPlan.$match[key])[1];
            if (key === 'timestamp') {
              filter += '.' + first_condition.replace('$', '') + '(' + queryPlan.$match[key][first_condition] + ')';
              filter += '.and(' + 'r.row("' + key + '").' + second_condition.replace('$', '') + '(' + queryPlan.$match[key][second_condition] + '))';
            }
            else {
              filter += '.' + first_condition.replace('$', '') + '("' + queryPlan.$match[key][first_condition] + '")';
              filter += '.and(' + 'r.row("' + key + '").' + second_condition.replace('$', '') + '("' + queryPlan.$match[key][second_condition] + '"))';
            }
          }
          else {
            first_condition = Object.keys(queryPlan.$match[key])[0];
            filter += '.' + first_condition.replace('$', '') + '("' + queryPlan.$match[key][first_condition] + '")';
          }
          filter += ')';
        });
        //filter += '}';
        //filter += '}';

        map = 'function map (doc){';
        map += 'return {';
        Object.keys(queryPlan.$map).forEach(function (key) {
          map += queryPlan.$map[key] + ',';
        });
        map = map.substring(0, map.length - 1);
        map += '}';
        map += '}';

        Object.keys(queryPlan.$sort).forEach(function (key) {
          if (queryPlan.$sort[key] === -1 || queryPlan.$sort[key].toString().toUpperCase() === 'DESC')
            sort += 'r.desc("' + key + '"),';
          else
            sort += 'r.asc("' + key + '"),';
        });
        sort = sort.substring(0, sort.length - 1);

        if (Object.keys(queryPlan.$group).length > 0) {
          group = 'function group (item){';
          group += 'return item.pluck(';
          Object.keys(queryPlan.$group).forEach(function (key) {
            group += '\'' + key + '\',';
          });
          group = group.substring(0, group.length - 1);
          group += ')';
          group += '}';
        }

        if (_plan.type !== 'ucount') {
          reduce = 'function reduce(left,right){';
          reduce += 'return {';
          Object.keys(queryPlan.$reduce).forEach(function (key) {
            reduce += queryPlan.$reduce[key] + ',';
          });
          reduce = reduce.substring(0, reduce.length - 1);
          reduce += '}';
          reduce += '}';

          final = '';

          final = 'function final(doc){';
          final += 'return {';
          Object.keys(queryPlan.$final).forEach(function (key) {
            final += queryPlan.$final[key] + ',';
          });
          final = final.substring(0, final.length - 1);
          final += '}';
          final += '}';

          if (Object.keys(queryPlan.$group).length > 0)
            statement = "r.db('" + self.options.db + "').table('" + _plan.table + "')" + filter + ".map(" + map + ").group(" + group + ").reduce(" + reduce + ").ungroup().map(" + final + ")";
          else if (Object.keys(queryPlan.$group).length === 0 && query.metrics[0].aggregation !== 'avg')
            statement = "r.db('" + self.options.db + "').table('" + _plan.table + "')" + filter + ".map(" + map + ").reduce(" + reduce + ")";
          else if (Object.keys(queryPlan.$group).length === 0 && query.metrics[0].aggregation === 'avg')
            statement = "r.db('" + self.options.db + "').table('" + _plan.table + "')" + filter + ".map(" + map + ").reduce(" + reduce + ").do(" + final + ")";
          if (sort.length > 0)
            statement += ".orderBy(" + sort + ")";
          if (queryPlan.$limit)
            statement += ".limit(" + queryPlan.$limit + ")";
          statement += ".run(self.client, " + cb.toString() + ")";
        }
        else {
          map2 = 'function map (doc){';
          map2 += 'return {';
          Object.keys(queryPlan.$group).forEach(function (key, index) {
            if (queryPlan.$group[key] !== 2) {
              if (Object.keys(queryPlan.$group).length > 1)
                map2 += key + ': doc("group")(' + index + '),';
              else
                map2 += key + ': doc("group"),';
            }
            //else
            map2 += 'count_' + key + ': 1,';
          });
          map2 = map2.substring(0, map2.length - 1);
          map2 += '}';
          map2 += '}';

          if (Object.keys(queryPlan.$group).length > 0) {
            group = ''
            Object.keys(queryPlan.$group).forEach(function (key) {
              group += '\'' + key + '\',';
            });
            group = group.substring(0, group.length - 1);

            group2 = ''
            Object.keys(queryPlan.$group).forEach(function (key) {
              if (queryPlan.$group[key] !== 2)
                group2 += '\'' + key + '\',';
            });
            group2 = group2.substring(0, group2.length - 1);
          }

          reduce = 'function reduce(left,right){';
          reduce += 'return {';
          Object.keys(queryPlan.$reduce).forEach(function (key) {
            reduce += queryPlan.$reduce[key] + ',';
          });
          reduce = reduce.substring(0, reduce.length - 1);
          reduce += '}';
          reduce += '}';

          final = 'function final(doc){';
          final += 'return {';
          Object.keys(queryPlan.$final).forEach(function (key) {
            final += queryPlan.$final[key] + ',';
          });
          final = final.substring(0, final.length - 1);
          final += '}';
          final += '}';

          if (Object.keys(queryPlan.$group).length > 0)
            statement = "r.db('" + self.options.db + "').table('" + _plan.table + "')" + filter + ".map(" + map + ").group(" + group + ").ungroup().map(" + map2 + ").group(" + group2 + ").reduce(" + reduce + ").ungroup().map(" + final + ")";
          else if (Object.keys(queryPlan.$group).length === 0)
            statement = "r.db('" + self.options.db + "').table('" + _plan.table + "')" + filter + ".map(" + map + ").group(" + group + ").ungroup().map(" + map2 + ").reduce(" + reduce + ")";
          else if (Object.keys(queryPlan.$group).length === 0 && query.metrics[0].aggregation === 'avg')
            statement = "r.db('" + self.options.db + "').table('" + _plan.table + "')" + filter + ".map(" + map + ").group(" + group + ").ungroup().map(" + map2 + ").reduce(" + reduce + ").do(" + final + ")";
          if (sort.length > 0)
            statement += ".orderBy(" + sort + ")";
          if (queryPlan.$limit)
            statement += ".limit(" + queryPlan.$limit + ")";
          statement += ".run(self.client, " + cb.toString() + ")";
        }

        var rql = statement;
        //console.log('rql', rql);
        eval(rql);
      };
      calls.push(call);
    });

    async.parallel(calls, function (err) {
      if (err) {
        if (err.toString().indexOf(' does not exist') === -1)
          return callback(err);
      }

      var output = {
        dimensions: query.dimensions,
        metrics: query.metrics,
        documents: [],
        queryplan: plan
      };
      //return callback(null, output);
      var keys = [];
      var final = [];

      if (results && results.length > 0) {
        results.forEach(function (_result) {
          _result = self.verifyResult(query, _result);
          if (!_result)
            return callback(null, output);

          _result.forEach(function (point) {
            var document = {_id: {}};
            Object.keys(point).forEach(function (col, i) {
              var dimension = _.find(query.dimensions, function (d) {
                return d.key === col.replace('.', '_');
              });
              if (dimension)
                document._id[col] = point[col];
              document[col] = point[col];
            });

            if (typeof document.timestamp === 'string')
              document.timestamp = parseInt(document.timestamp);
            if (document.timestamp > 0)
              document.timestamp = new Date(document.timestamp);

            var key = self.common.hash(JSON.stringify(document._id));
            var row;
            if (keys.indexOf(key) === -1) {
              row = {};
              Object.keys(document._id).forEach(function (key) {
                row[key] = document._id[key];
              });
              row.key = key;
              keys.push(key);
              final.push(row);
            }
            else {
              row = _.find(final, function (f) {
                return f.key == key;
              });
              if (!row) //TODO: how can this happen?
              {
                console.log('wtf?', key, query);
              }
            }
            Object.keys(document).forEach(function (attribute) {
              if (attribute != '_id') {
                if (document.hasOwnProperty(attribute)) {
                  if (attribute.indexOf('.') > -1) {
                    row[attribute.replace('.', '_')] = document[attribute];
                    delete row[attribute];
                  }
                  else
                    row[attribute] = document[attribute];
                }
                else
                  row[attribute] = '(not set)';
              }
            });
            output.metrics.forEach(function (m) {
              if (!row[m.key])
                row[m.key] = null;
            });

            //delete row.key;
            final[keys.indexOf(key)] = row;
          });
        });

        output.documents = final;

        //console.log('final', output);

        return setImmediate(function () {
          return callback(null, output);
        });
      }
      else {
        output.dimensions = plan.dimensions;

        output.metrics = plan.metrics;
        output.documents = [];
        return setImmediate(function () {
          return callback(null, output);
        });
      }
    });
  });
};

RethinkDBProvider.prototype.verifyResult = function (query, result) {
  //console.log(result);

  return result;
};

RethinkDBProvider.prototype.openConnection = function (options, callback) {
  callback = callback || function () {
  };

  var self = this;
  r.connect(options, function (err, conn) {
    if (err)
      return callback(err);

    self.client = conn;
    return self.checkConnection(null, callback);
  });
};

RethinkDBProvider.prototype.closeConnection = function (connection, callback) {
  callback = callback || function () {
  };

  var self = this;
  self.client.close();
  return callback(null);
};

RethinkDBProvider.prototype.checkConnection = function (connection, callback) {
  callback = callback || function () {
  };
  var self = this;
  r.db(self.options.db).tableList().run(self.client, function (err) {
    if (err)
      return callback(err);

    return callback(null, connection);
  });
};

RethinkDBProvider.prototype.stats = function (collection, callback) {
  var self = this;

  r.db(self.options.db).table(collection).count().run(self.client, function (err, result) {
    return callback(null, {count: result});
  });

  /*
   self.client.query('select count(f) from ' + collection.replace(/-/ig, '_'), function (err, result) {
   if (err)
   return callback(err);

   return callback(null, {count: result});
   });*/
};

RethinkDBProvider.prototype.drop = function (collection, callback) {
  var self = this;

  r.db(self.options.db).tableDrop(collection.replace(/[^\w\s]/gi, '')).run(self.client, function (err) {
    if (err)
      return callback(err);

    return callback(null);
  });
};

RethinkDBProvider.prototype.purge = function (callback) {
  var self = this;

  r.dbDrop(self.options.db).run(self.client, function (err) {
    if (err)
      return callback(err);

    return callback(null);
  });
};