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
  r.db(self.options.db).tableCreate(collection.storeKey).run(self.client, function (err, result) {
    //ignore errors if table already exists
    r.table(collection.storeKey).insert(documents).run(self.client, function (err, result) {
      if (err)
        return callback(err);
      return callback(null, result);
    });
  });
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

  if (!query.dimensions)
    query.dimensions = [];
  if (!query.metrics)
    query.metrics = [];

  if (query.timeframe && !query.timeframe.hasOwnProperty('last_n_items')) {
    if (typeof query.timeframe.start === 'string')
      query.timeframe.start = new Date(query.timeframe.start);
    if (typeof query.timeframe.end === 'string')
      query.timeframe.end = new Date(query.timeframe.end);
    $match.time = {$gt: query.timeframe.start.toISOString().replace('T', ' ').replace('Z', ''), $lt: query.timeframe.end.toISOString().replace('T', ' ').replace('Z', '')};
  }
  else if (query.timeframe && query.timeframe.hasOwnProperty('last_n_items')) {
    $limit = query.timeframe.last_n_items;
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

  switch (query.interval) {
    case 'timebucket.second':
      query.interval = 's';
      break;
    case 'timebucket.minute':
      query.interval = 'm';
      break;
    case 'timebucket.hour':
      query.interval = 'h';
      break;
    case 'timebucket.ddate':
      query.interval = 'd';
      break;
    case 'timebucket.week':
      query.interval = 'w';
      break;
    default:
      break;
  }

  query.dimensions.forEach(function (dimension) {
    switch (dimension.datatype) {
      case 'date':
        $project.time = 'time';
        $final.time = 'time(1' + query.interval + ')';
        query.timeSeries = true;
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
      table: metric.collection.storeKey,
      query: []
    };

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
    if (!metric.formula && metric.collection) {
      metric.aggregation = metric.aggregation || 'sum';
      if (metric.aggregation == 'ucount')
        colQuery.type = 'ucount';
      else
        colQuery.type = 'plain';

      colQuery.key = self.common.hash(colQuery.type + '_' + metric.collection.key + '_' + JSON.stringify(_$match));
      if (plan.colQueries[colQuery.key]) {
        _$final = self.common.extend({}, plan.colQueries[colQuery.key].query.$final);
        _$final = self.common.extend(plan.colQueries[colQuery.key].query.$final, _$final);
      }

      //let's iterate over the dimensions again and make sure the table holds them
      Object.keys(_$group).forEach(function (grouped) {
        var exist = _.find(metric.collection.dimensions, function (dimension) {
          return dimension.key === grouped;
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
          _$map[grouped] = grouped + ': doc("' + grouped + '")';
          _$reduce[grouped] = grouped + ': left("' + grouped + '")';
        }
      });

      if (metric.key !== 'fake') {
        metric.aggregation = metric.aggregation || 'sum';
        switch (metric.aggregation) {
          case 'avg':
            _$map['sum_' + metric.dependsOn] = 'sum_' + metric.dependsOn + ': doc("' + metric.dependsOn + '")';
            _$map['count_' + metric.dependsOn] = 'count_' + metric.dependsOn + ': 1';
            _$reduce['sum_' + metric.dependsOn] = 'sum_' + metric.dependsOn + ': left("sum_' + metric.dependsOn + '").add(right("sum_' + metric.dependsOn + '"))';
            _$reduce['count_' + metric.dependsOn] = 'count_' + metric.dependsOn + ': left("count_' + metric.dependsOn + '").add(right("count_' + metric.dependsOn + '"))';
            _$final[metric.key] = metric.key + ': doc("reduction")("sum_' + metric.attribute + '").div(doc("reduction")("count_' + metric.attribute + '"))';
            break;
          case 'distinct':
          case 'unique':
          case 'ucount':
            _$map[metric.dependsOn] = metric.dependsOn + ': doc("' + metric.dependsOn + '")';
            _$group[metric.dependsOn] = 2;
            _$reduce['count_' + metric.dependsOn] = 'count_' + metric.dependsOn + ': left("' + 'count_' + metric.dependsOn + '").add(right("' + 'count_' + metric.dependsOn + '"))';
            _$final[metric.key] = metric.key + ': doc("reduction")("count_' + metric.dependsOn + '")';
            break;
          case 'sum':
          default:
            _$map[metric.aggregation + '_' + metric.dependsOn] = metric.aggregation + '_' + metric.dependsOn + ': doc("' + metric.dependsOn + '")';
            _$reduce[metric.aggregation + '_' + metric.dependsOn] = metric.aggregation + '_' + metric.dependsOn + ': left("' + metric.aggregation + '_' + metric.dependsOn + '").add(right("' + metric.aggregation + '_' + metric.dependsOn + '"))';
            _$final[metric.key] = metric.key + ': doc("reduction")("' + metric.aggregation + '_' + metric.attribute + '")';
            break;
        }
      }

      colQuery.query = {
        $match: _$match,
        $sort: {time: -1},
        $group: _$group,
        $map: _$map,
        $reduce: _$reduce,
        $final: _$final
      };
      if ($limit) {
        colQuery.query.$limit = $limit;
      }

      plan.colQueries[colQuery.key] = colQuery;
    }

  });

  console.log('plan', require('util').inspect(plan.colQueries, {depth: null, colors: true}));

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
          if (err)
            return callback(err);

          cursor.toArray(function (err, result) {
            if (err) {
              return callback(err);
            }
            else {
              console.log(require('util').inspect(result, {depth: null, colors: true}));
              results.push(result)
              return callback(null, result);
            }
          })
        };

        var filter = 'function filter (){';
        filter += 'return {';
        Object.keys(queryPlan.$match).forEach(function (key) {

        });
        filter += '}';
        filter += '}';

        var map = 'function map (doc){';
        map += 'return {';
        Object.keys(queryPlan.$map).forEach(function (key) {
          map += queryPlan.$map[key] + ',';
        });
        map = map.substring(0, map.length - 1);
        map += '}';
        map += '}';

        var group = 'function group (item){';
        group += 'return item.pluck(';
        Object.keys(queryPlan.$group).forEach(function (key) {
          group += '\'' + key + '\',';
        });
        group = group.substring(0, group.length - 1);
        group += ')';
        group += '}';

        if (_plan.type !== 'ucount') {
          var reduce = 'function reduce(left,right){';
          reduce += 'return {';
          Object.keys(queryPlan.$reduce).forEach(function (key) {
            reduce += queryPlan.$reduce[key] + ',';
          });
          reduce = reduce.substring(0, reduce.length - 1);
          reduce += '}';
          reduce += '}';

          var final = '';

          final = 'function final(doc){';
          final += 'return {';
          Object.keys(queryPlan.$final).forEach(function (key) {
            final += queryPlan.$final[key] + ',';
          });
          final = final.substring(0, final.length - 1);
          final += '}';
          final += '}';

          var statement = "r.db('" + self.options.db + "').table('" + _plan.table + "').filter(" + filter + ").map(" + map + ").group(" + group + ").reduce(" + reduce + ").ungroup().map(" + final + ").run(self.client, " + cb.toString() + ")";
        }
        else {
          var map2 = 'function map (doc){';
          map2 += 'return {';
          Object.keys(queryPlan.$group).forEach(function (key, index) {
            if (queryPlan.$group[key] !== 2)
              map2 += key + ': doc("group")(' + index + '),';
            else
              map2 += 'count_' + key + ': 1,';
          });
          map2 = map2.substring(0, map2.length - 1);
          map2 += '}';
          map2 += '}';

          group = ''
          Object.keys(queryPlan.$group).forEach(function (key) {
            group += '\'' + key + '\',';
          });
          group = group.substring(0, group.length - 1);

          var group2 = ''
          Object.keys(queryPlan.$group).forEach(function (key) {
            if (queryPlan.$group[key] !== 2)
              group2 += '\'' + key + '\',';
          });
          group2 = group2.substring(0, group2.length - 1);

          var reduce = 'function reduce(left,right){';
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

          console.log('map', map);
          var statement = "r.db('" + self.options.db + "').table('" + _plan.table + "').filter(" + filter + ").map(" + map + ").group(" + group + ").ungroup().map(" + map2 + ").group(" + group2 + ").reduce(" + reduce + ").ungroup().map(" + final + ").run(self.client, " + cb.toString() + ")";
        }

        var rql = statement;
        console.log(rql);
        //rql += cb.toString();
        eval(rql);
        //r.js(rql).run(self.client, cb);
      };
      calls.push(call);
    });

    async.parallel(calls, function (err) {
      if (err)
        return callback(err);


      var output = {
        dimensions: query.dimensions,
        metrics: query.metrics,
        documents: [],
        queryplan: plan
      };
      return callback(null, output);
      var keys = [];
      var final = [];

      if (results && results.length > 0) {
        results.forEach(function (_result) {
          _result = _result[0];
          _result = self.verifyResult(query, _result);
          if (!_result)
            return callback(null, output);
          if (!_result.points)
            _result.points = [];

          var timeIndex = _result.columns.lastIndexOf('time');
          if (timeIndex > -1)
            _result.columns[timeIndex] = 'timestamp';

          _result.points.forEach(function (point) {
            var document = {_id: {}};
            _result.columns.forEach(function (col, i) {
              var dimension = _.find(query.dimensions, function (d) {
                return d.key === col.replace('.', '_');
              });
              if (dimension)
                document._id[col] = point[i];
              document[col] = point[i];
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

        //console.log(final);

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
  return callback(null, connection);
};

RethinkDBProvider.prototype.stats = function (collection, callback) {
  var self = this;

  self.client.query('select count(f) from ' + collection.replace(/-/ig, '_'), function (err, result) {
    if (err)
      return callback(err);

    return callback(null, {count: result});
  });
};

RethinkDBProvider.prototype.drop = function (collection, callback) {
  var self = this;

  self.client.query('drop series ' + collection.replace(/-/ig, '_'), function (err) {
    if (err)
      return callback(err);

    return callback(null);
  });
};

RethinkDBProvider.prototype.purge = function (callback) {
  var self = this;

  self.client.deleteDatabase('joola', function (err) {
    if (err)
      return callback(err);

    return callback(null);
  });
};