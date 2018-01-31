'use strict';

var util = require('util');
var BaseProducer = require('./baseProducer');

/** @inheritdoc */
function HighLevelProducer (client, options, partitionerArgs) {
  BaseProducer.call(this, client, options, BaseProducer.PARTITIONER_TYPES.cyclic, partitionerArgs);
}

util.inherits(HighLevelProducer, BaseProducer);

HighLevelProducer.PARTITIONER_TYPES = BaseProducer.PARTITIONER_TYPES;

module.exports = HighLevelProducer;
