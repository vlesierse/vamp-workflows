'use strict';

let _ = require('highland');
let vamp = require('vamp-node-client');

let api = new vamp.Api();
var http = new vamp.Http();
let logger = new vamp.Log();

let findOldIndices = function(elasticsearch, period, indexPrefixes) {
  var days = Array.apply(null, {length: period}).map(Number.call, Number);
  let validIndices = indexPrefixes.map(indexPrefix => {
    return days.map(day => {
      let date = new Date();
      date.setDate(date.getDate() - day);
      return `${indexPrefix}-${date.getUTCFullYear()}-${('0' + (date.getUTCMonth() + 1)).slice(-2)}-${('0' + (date.getUTCDate())).slice(-2)}`
    });
  }).reduce((a, b) => a.concat(b));
  return _(http.get(elasticsearch + '/_cat/indices?v', { headers: { 'Content-Type': 'application/json' } }).then(JSON.parse)).flatMap(indices => {
    return indices.map(i => i.index).filter(index => indexPrefixes.some(i => index.startsWith(i)) && !validIndices.includes(index));
  });
}

let deleteIndex = function(elasticsearch, index) {
  logger.log(`Delete index '${index}'`);
  return _(http.request(`${elasticsearch}/${index}`, { method: 'DELETE' }));
}

api.config().flatMap(function (config) {
  let elasticsearch = process.env.VAMP_ELASTICSEARCH_URL || config['vamp.pulse.elasticsearch.url'];
  let period = Number(process.env.RETENTION_PERIOD || '1');
  let indexPrefixes = (process.env.RETENTION_INDEX || '').split(',');
  return findOldIndices(elasticsearch, period, indexPrefixes).flatMap(index => deleteIndex(elasticsearch, index));
}).each(() => {});
