/* jshint sub: true */
var r = require("rethinkdb");
var moment = require("moment");
var async = require("async");
var _ = require("lodash-node");
var util = require("util");

var Connector = require("loopback-connector").Connector;

exports.initialize = function initializeDataSource(dataSource, callback) {
    if (!r) return;

    var s = dataSource.settings;

    if (dataSource.settings.rs) {

        s.rs = dataSource.settings.rs;
        if (dataSource.settings.url) {
            var uris = dataSource.settings.url.split(',');
            s.hosts = [];
            s.ports = [];
            uris.forEach(function(uri) {
                var url = require('url').parse(uri);

                s.hosts.push(url.hostname || 'localhost');
                s.ports.push(parseInt(url.port || '28015', 10));

                if (!s.database) s.database = url.pathname.replace(/^\//, '');
                if (!s.username) s.username = url.auth && url.auth.split(':')[0];
                if (!s.password) s.password = url.auth && url.auth.split(':')[1];
            });
        }

        s.database = s.database || 'test';

    } else {

        if (dataSource.settings.url) {
            var url = require('url').parse(dataSource.settings.url);
            s.host = url.hostname;
            s.port = url.port;
            s.database = url.pathname.replace(/^\//, '');
            s.username = url.auth && url.auth.split(':')[0];
            s.password = url.auth && url.auth.split(':')[1];
        }

        s.host = s.host || 'localhost';
        s.port = parseInt(s.port || '28015', 10);
        s.database = s.database || 'test';

    }

    s.safe = s.safe || false;

    dataSource.adapter = new RethinkDB(s, dataSource);
    dataSource.connector = dataSource.adapter

    if (callback) {
        dataSource.connector.connect(callback);
    }

    process.nextTick(callback);
};

function RethinkDB(s, dataSource) {
    Connector.call(this, "rethink", s);
    this.dataSource = dataSource;
    this.database = s.database;
}

util.inherits(RethinkDB, Connector);

RethinkDB.prototype.connect = function(cb) {
    var self = this
    var s = self.settings
    if (self.db) {
        process.nextTick(function () {
            cb && cb(null, self.db);
        });
    } else {
        r.connect({host: s.host, port: s.port}, function (error, client) {
            self.db = client
            cb && cb(error, client)
        });
    }
};

RethinkDB.prototype.getTypes = function () {
  return ["db", "nosql", "rethinkdb"];
};

RethinkDB.prototype.getDefaultIdType = function () {
  return String;
};

RethinkDB.prototype.table = function(model) {
    return this._models[model].model.tableName;
};

// creates tables if not exists
RethinkDB.prototype.autoupdate = function(models, done) {
    var _this = this;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.autoupdate(models, done);
        });
        return
    }

    if ((!done) && ('function' === typeof models)) {
      done = models;
      models = undefined;
    }

    // First argument is a model name
    if ('string' === typeof models) {
      models = [models];
    }

    models = models || Object.keys(_this._models);

    r.db(_this.database).tableList().run(client, function(error, cursor) {
        if (error) {
            return done(error);
        }

        cursor.toArray(function(error, list) {
            async.each(models, function(model, cb) {
                if (list.length === 0 || list.indexOf(model) < 0) {
                    r.db(_this.database).tableCreate(model).run(client, function(error) {
                        if (error) return cb(error);
                        createIndices(cb, model, client);
                    });
                }
                else {
                    createIndices(cb, model, client);
                }
            }, function(e) {
                done(e);
            });
        });
        
    });

    function createIndices(cb, model, client) {
        var properties = _this._models[model].properties;
        var settings = _this._models[model].settings;
        var indexCollection = _.extend({}, properties, settings);

        function checkAndCreate(list, indexName, indexOption, indexFunction, cb3) {

            // Don't attempt to create an index on primary key 'id'
            if (indexName !== 'id' && _hasIndex(_this, model, indexName) && list.indexOf(indexName) < 0) {
                var query = r.db(_this.database).table(model);
                if (indexFunction) {
                    query = query.indexCreate(indexName, indexFunction, indexOption);
                }
                else {
                    query = query.indexCreate(indexName, indexOption);
                }
                query.run(client, cb3);
            }
            else {
                cb3();
            }
        }

        if (!_.isEmpty(indexCollection)) {
            r.db(_this.database).table(model).indexList().run(client, function(error, cursor) {
                if (error) return cb(error);

                cursor.toArray(function(error, list) {
                    if (error) return cb(error);

                    async.each(Object.keys(indexCollection), function (indexName, cb4) {
                        var indexConf = indexCollection[indexName];
                        checkAndCreate(list, indexName, indexConf.indexOption || {}, indexConf.indexFunction, cb4);
                    }, function(err) {
                        cb(err);
                    });
                });
            });
        } else {
            cb();
        }
    }
};

// drops tables and re-creates them
RethinkDB.prototype.automigrate = function(models, cb) {
    this.autoupdate(models, cb);
};

// checks if database needs to be actualized
RethinkDB.prototype.isActual = function(cb) {
    var _this = this;
    var client = this.db;

    r.db(_this.database).tableList().run(client, function(error, cursor) {
        if (error) return cb(error)
        if (!cursor.next()) return cb(null, _.isEmpty(_this._models))

        cursor.toArray(function(error, list) {
            if (error) {
                return cb(error);
            }
            var actual = true;
            async.each(Object.keys(_this._models), function(model, cb2) {
                if(!actual) return cb2();

                var properties = _this._models[model].properties;
                var settings = _this._models[model].settings;
                var indexCollection = _.extend({}, properties, settings);
                if (list.indexOf(model) < 0) {
                    actual = false;
                    cb2();
                } else {
                    r.db(_this.database).table(model).indexList().run(client, function(error, cursor) {
                        if (error) return cb2(error);

                        cursor.toArray(function(error, list) {
                            if (error || !actual) return cb2(error);


                            Object.keys(indexCollection).forEach(function (property) {
                                if (_hasIndex(_this, model, property) && list.indexOf(property) < 0)
                                    actual = false;
                            });
                            cb2();
                        });
                    });
                }
            }, function(err) {
                cb(err, actual);
            });
        });
    });
};

RethinkDB.prototype.create = function (model, data, callback) {
    if (data.id === null || data.id === undefined) {
        delete data.id;
    }

    this.save(model, data, callback, true);
};

RethinkDB.prototype.updateOrCreate = function (model, data, callback) {
    if (data.id === null || data.id === undefined) {
        delete data.id;
    }

    this.save(model, data, callback, true, true);
}

RethinkDB.prototype.save = function (model, data, callback, strict, returnObject) {
    var _this = this;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.save(model, data, callback, strict, returnObject);
        });
        return
    }

    if (strict == undefined)
        strict = false

    Object.keys(data).forEach(function (key) {
        if (data[key] === undefined)
            data[key] = null;
    });

    r.db(_this.database).table(model).insert(data, { conflict: strict ? "error": "update", returnChanges: true }).run(client, function (err, m) {
        err = err || m.first_error && new Error(m.first_error);
        if (err)
            callback && callback(err)
        else {
            var info = {}
            var id = model.id

            //if (m.inserted > 0) info.isNewInstance = true
            if (m.changes && m.changes.length > 0) id = m.changes[0].new_val.id

            if (returnObject && m.changes && m.changes.length > 0) {
                callback && callback(null, m.changes[0].new_val, info)
            } else {                
                callback && callback(null, id, info);
            }
        }
    });
};

RethinkDB.prototype.exists = function (model, id, callback) {
    var _this = this;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.exists(model, id, callback);
        });
        return
    }

    r.db(_this.database).table(model).get(id).run(client, function (err, data) {
        callback(err, !!(!err && data));
    });
};

RethinkDB.prototype.find = function find(model, id, options, callback) {
    var _this = this,
        _keys;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.find(model, id, callback);
        });
        return
    }

    var done = function (client) {

        return function finished(err, data) {

            // Acquire the keys for this model
            _keys = _this._models[model].properties;

            if (data) {

                // Pass to expansion helper
                _expandResult(data, _keys);
            }

            // Done
            callback(err, data);
        };
    };

    r.db(_this.database)
        .table(model)
        .get(id)
        .run(client, done(client));
};

RethinkDB.prototype.destroy = function destroy(model, id, callback) {
    var _this = this;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.destroy(model, id, callback);
        });
        return
    }

    r.db(_this.database).table(model).get(id).delete().run(client, function(error, result) {
        callback(error);
    });
};

RethinkDB.prototype.all = function all(model, filter, options, callback) {
    var _this = this;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.all(model, filter, options, callback);
        });
        return
    }

    if (!filter) {
        filter = {};
    }

    var promise = r.db(_this.database).table(model);

    if (filter.where) {
        promise = buildWhere(_this, model, filter.where, promise)//_processWhere(_this, model, filter.where, promise);
    }

    if (filter.order) {
        var keys = filter.order;
        if (typeof keys === 'string') {
            keys = keys.split(',');
        }
        keys.forEach(function(key) {
            var m = key.match(/\s+(A|DE)SC$/);
            key = key.replace(/\s+(A|DE)SC$/, '').trim();
            if (m && m[1] === 'DE') {
                promise = promise.orderBy(r.desc(key));
            } else {
                promise = promise.orderBy(r.asc(key));
            }
        });
    } else {
        // default sort by id
        promise = promise.orderBy(r.asc("id"));
    }

    if (filter.skip) {
        promise = promise.skip(filter.skip);
    } else if (filter.offset) {
        promise = promise.skip(filter.offset);
    }
    if (filter.limit) {
        promise = promise.limit(filter.limit);
    }

    //console.log(promise.toString())

    promise.run(client, function(error, cursor) {

        if (error || !cursor) {
            return callback(error, null);
        }

        _keys = _this._models[model].properties;
        _model = _this._models[model].model;

        cursor.toArray(function (err, data) {
            if (err) {
                return callback(err);
            }

            data.forEach(function(element, index) {
                _expandResult(element, _keys);
            });

            if (filter && filter.include && filter.include.length > 0) {
                _model.include(data, filter.include, options, callback);
            } else {
                callback && callback(null, data);
            }
        });
    });
};

RethinkDB.prototype.destroyAll = function destroyAll(model, where, callback) {
    var _this = this;
    var client = this.db;

    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.destroyAll(model, where, callback);
        });
        return
    }

    if (!callback && "function" === typeof where) {
        callback = where
        where = undefined
    }

    var promise = r.db(_this.database).table(model)
    if (where !== undefined)
        promise = buildWhere(_this, model, where, promise)

    promise.delete().run(client, function(error, result) {
        callback(error, { count: result ? result.deleted : null});
    });
};

RethinkDB.prototype.count = function count(model, callback, where) {
    var _this = this;
    var client = this.db;


    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.count(model, callback, where);
        });
        return
    }

    var promise = r.db(_this.database).table(model);

    if (where && typeof where === "object")
        promise = buildWhere(_this, model, where, promise);

    promise.count().run(client, function (err, count) {
        callback(err, count);
    });
};

RethinkDB.prototype.updateAttributes = function updateAttrs(model, id, data, cb) {
    var _this = this;
    var client = this.db;


    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.updateAttributes(model, id, data, callback);
        });
        return
    }

    data.id = id;
    Object.keys(data).forEach(function (key) {
        if (data[key] === undefined)
            data[key] = null;
    });
    r.db(_this.database).table(model).update(data).run(client, function(err, object) {
        cb(err, data);
    });
};

RethinkDB.prototype.update = RethinkDB.prototype.updateAll = function update(model, where, data, callback) {
    var _this = this;
    var client = this.db;


    if (!client) {
        _this.dataSource.once('connected', function () {
            _this.update(model, where, data, callback);
        });
        return
    }

    var promise = r.db(_this.database).table(model)
    if (where !== undefined)
        promise = buildWhere(_this, model, where, promise)
    promise.update(data, { returnChanges: true }).run(client, function(error, result) {
        callback(error, { count: result ? result.replaced : null });
    });
}

RethinkDB.prototype.disconnect = function () {
    this.db.close()
    this.db = null
};

/*
    Some values may require post-processing. Do that here.
*/
function _expandResult(result, keys) {

    Object.keys(result).forEach(function (key) {

        if (!keys.hasOwnProperty(key)) return;

        if (keys[key]['type'] &&
            keys[key]['type']['name'] === 'Date' &&
            !(result[key] instanceof Date)) {

            // Expand date result data, backward compatible
            result[key] = moment.unix(result[key]).toDate();
        }
    });
}

function _hasIndex(_this, model, key) {

    // Primary key always hasIndex
    if (key === 'id') return true;

    var modelDef = _this._models[model];
    return (_.isObject(modelDef.properties[key]) && modelDef.properties[key].index) ||
        (_.isObject(modelDef.settings[key]) && modelDef.settings[key].index);
}

function _toMatchExpr(regexp) {
    var expr = regexp.toString(),
        exprStop = expr.lastIndexOf('/'),
        exprCi = expr.slice(exprStop).search('i');

    expr = expr.slice(1, exprStop);

    if (exprCi > -1) {
        expr = '(?i)' + expr;
    }

    return expr;
}

function _matchFn(k, cond) {

    var matchExpr = _toMatchExpr(cond);
    return function (row) {
        return row(k).match(matchExpr);
    };
}

function _inqFn(k, cond) {

    return function (row) {
        return r.expr(cond).contains(row(k));
    };
}

var operators = {
    "between": function(key, value) {
        return r.row(key).gt(value[0]).and(r.row(key).lt(value[1]))
    },
    "gt": function(key, value) {
        return r.row(key).gt(value)
    },
    "lt": function(key, value) {
        return r.row(key).lt(value)
    },
    "gte": function(key, value) {
        return r.row(key).ge(value)
    },
    "lte": function(key, value) {
        return r.row(key).le(value)
    },
    "inq": function(key, value) {
        var query = []

        value.forEach(function(v) {
            query.push(r.row(key).eq(v))
        })

        var condition = _.reduce(query, function(sum, qq) {
            return sum.or(qq)
        })

        return condition
    },
    "nin": function(key, value) {
        var query = []

        value.forEach(function(v) {
            query.push(r.row(key).ne(v))
        })

        var condition = _.reduce(query, function(sum, qq) {
            return sum.and(qq)
        })

        return condition
    },
    "neq": function(key, value) {
        return r.row(key).ne(value)
    },
    "like": function(key, value) {
        return r.row(key).match(value)
    },
    "nlike": function(key, value) {
        return r.row(key).match(value).not()
    }
}

function buildFilter(where) {
    var filter = []

    Object.keys(where).forEach(function(k) {

        // determine if k is field name or condition name
        var conditions = ["and", "or", "between", "gt", "lt", "gte", "lte", "inq", "nin", "near", "neq", "like", "nlike"]
        var condition = where[k]

        if (k === "and" || k === "or") {
            if (_.isArray(condition)) {
                var query = _.map(condition, function(c) {
                    return buildFilter(c)
                })

                if (k === "and")
                    filter.push(_.reduce(query, function(s, f) {
                        return s.and(f)
                    }))
                else
                    filter.push(_.reduce(query, function(s, f) {
                        return s.or(f)
                    }))
            }
        } else {
            if (_.isObject(condition) && _.intersection(_.keys(condition), conditions).length > 0) {
                // k is condition
                _.keys(condition).forEach(function(operator) {
                    if (conditions.indexOf(operator) >= 0) {
                        filter.push(operators[operator](k, condition[operator]))
                    }
                })
            } else {
                // k is field equality
                filter.push(r.row(k).eq(condition))
            }
        }    

    })

    return _.reduce(filter, function(s, f) {
        return s.and(f)
    })
}

function buildWhere(self, model, where, promise) {

    if (where === null || (typeof where !== 'object')) {
        return promise;
    }

    var query = buildFilter(where)

    if (query)
        return promise.filter(query)
    else
        return promise

}
