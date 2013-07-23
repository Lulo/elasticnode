var request = require('request');

var ElasticNode = function (config) {
  this.host = config.host;
  this.port = config.port;
};

ElasticNode.prototype.index = function (options, routing, callback) {
  if (!options.doc || !options.doc.id) {
    callback(new Error('Must provide a document for indexing. That document must have an id field.'));
    return;
  }
  if(!options.index || !options.type) {
    callback(new Error('Must provide an index and type for indexing.'));
    return;
  }
  // keep support for previous function signature
  if (arguments.length == 2) {
    callback = routing;
    routing = '';
  }
  if (routing) {
    routing = '?routing=' + routing;
  }
  var requestObject = {
    uri: 'http://' + this.host + ':' + this.port + '/' + options.index + '/' + options.type + '/' + options.doc.id + routing,
    method: 'PUT',
    body: JSON.stringify(options.doc)
  };
  request(requestObject, callback);
};

ElasticNode.prototype.get = function (options, routing, callback) {
  if (!options.index || !options.type || !options.id) {
    callback(new Error('Must provide index, type, and id to GET the document you want'));
    return;
  }
  // keep support for previous function signature
  if (arguments.length == 2) {
    callback = routing;
    routing = '';
  }
  if (routing) {
    routing = '?routing=' + routing;
  }
  var requestObject = {
    uri: 'http://' + this.host + ':' + this.port + '/' + options.index + '/' + options.type + '/' + options.id + routing,
  };
  request(requestObject, callback);
};

ElasticNode.prototype.search = function (options, routing, callback) {
  if (!options.index || !options.type) {
    callback(new Error('Must provide index and type to perform a search'));
    return;
  }
  if (!options.queryObject) {
    callback(new Error('Must provide a query object to perform a search'));
  }
  // keep support for previous function signature
  if (arguments.length == 2) {
    callback = routing;
    routing = '';
  }
  if (routing) {
    routing = '?routing=' + routing;
  }
  var requestObject = {
    uri: 'http://' + this.host + ':' + this.port + '/' + options.index + '/' + options.type + '/_search' + routing,
    method: 'GET',
    body: JSON.stringify(options.queryObject)
  };
  request(requestObject, callback);
};

ElasticNode.prototype.moreLikeThis = function (options, routing, callback) {
  if (!options.index || !options.type || !options.id) {
    callback(new Error('Must provide index, type, and id to perform a moreLikeThis query'));
    return;
  }
  // keep support for previous function signature
  if (arguments.length == 2) {
    callback = routing;
    routing = '';
  }
  if (routing) {
    routing = '?routing=' + routing;
  }
  var requestObject = {
    uri: 'http://' + this.host + ':' + this.port + '/' + options.index + '/' + options.type + '/' + options.id + '/_mlt' + routing,
    method: 'GET',
    body: JSON.stringify(options.queryObject),
    qs: {
      search_from: options.from || 0,
      search_size: options.size || 10,
    }
  };
  if (options.mlt_fields) requestObject.qs.mlt_fields = options.mlt_fields;
  request(requestObject, callback);
};

ElasticNode.prototype.deleteByQuery = function (options, routing, callback) {
  if (!options.index || !options.type) {
    callback(new Error('Must provide index and type to perform a deletion by query'));
    return;
  }
  if (!options.queryObject) {
    callback(new Error('Must provide a query object to perform a deletion by query'));
    return;
  }
  // keep support for previous function signature
  if (arguments.length == 2) {
    callback = routing;
    routing = '';
  }
  if (routing) {
    routing = '?routing=' + routing;
  }
  var requestObject = {
    uri: 'http://' + this.host + ':' + this.port + '/' + options.index + '/_query' + routing,
    method: 'DELETE',
    body: JSON.stringify(options.queryObject)
  };
  request(requestObject, callback);
};

module.exports = ElasticNode;

