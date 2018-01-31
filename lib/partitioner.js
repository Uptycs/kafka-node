'use strict';

var util = require('util');
var _ = require('lodash');

var Partitioner = function () {};

var DefaultPartitioner = function () {};
util.inherits(DefaultPartitioner, Partitioner);

DefaultPartitioner.prototype.getPartition = function (topicMetadata) {
  if (!topicMetadata) {
    return 0;
  }

  const partitions = _.map(topicMetadata, 'partition');
  if (partitions && _.isArray(partitions) && partitions.length > 0) {
    return partitions[0];
  }
  return 0;
};

var CyclicPartitioner = function () {
  this.c = 0;
};
util.inherits(CyclicPartitioner, Partitioner);

CyclicPartitioner.prototype.getPartition = function (topicMetadata) {
  if (!topicMetadata) {
    return 0;
  }

  const partitions = _.map(topicMetadata, 'partition');
  if (partitions && _.isArray(partitions) && partitions.length > 0) {
    return partitions[ this.c++ % partitions.length ];
  }
  return 0;
};

var RandomPartitioner = function () {};
util.inherits(RandomPartitioner, Partitioner);

RandomPartitioner.prototype.getPartition = function (topicMetadata) {
  if (!topicMetadata) {
    return 0;
  }

  const partitions = _.map(topicMetadata, 'partition');
  if (partitions && _.isArray(partitions) && partitions.length > 0) {
    return partitions[Math.floor(Math.random() * partitions.length)];
  }
  return 0;
};

var KeyedPartitioner = function () {};
util.inherits(KeyedPartitioner, Partitioner);

// Taken from oid package (Dan Bornstein)
// Copyright The Obvious Corporation.
function hashCode (string) {
  var hash = 0;
  var length = string.length;

  for (var i = 0; i < length; i++) {
    hash = ((hash * 31) + string.charCodeAt(i)) & 0x7fffffff;
  }

  return (hash === 0) ? 1 : hash;
};

KeyedPartitioner.prototype.getPartition = function (topicMetadata, payload) {
  if (!topicMetadata) {
    return 0;
  }

  const partitions = _.map(topicMetadata, 'partition');
  if (partitions && _.isArray(partitions) && partitions.length > 0) {
    const key = payload && payload.key ? payload.key : '';
    const index = hashCode(key) % partitions.length;
    return partitions[index];
  }
  return 0;
};

var CustomPartitioner = function (partitioner) {
  this.getPartition = partitioner;
};
util.inherits(CustomPartitioner, Partitioner);

var RackawarePartitioner = function (rack) {
  this.rack = rack;
};
util.inherits(RackawarePartitioner, Partitioner);

RackawarePartitioner.prototype.getPartition = function (topicMetadata, payload) {
  if (!topicMetadata) {
    return 0;
  }

  let rackMetadata = _.filter(topicMetadata, partition => {
    return this.rack === partition.rack;
  });
  if (!rackMetadata || !_.isArray(rackMetadata) || rackMetadata.length < 1) {
    rackMetadata = topicMetadata;
  }

  const partitions = _.map(rackMetadata, 'partition');
  if (partitions && _.isArray(partitions) && partitions.length > 0) {
    const key = payload && payload.key ? payload.key : '';
    const index = hashCode(key) % partitions.length;
    return partitions[index];
  }
  return 0;
};

module.exports.DefaultPartitioner = DefaultPartitioner;
module.exports.CyclicPartitioner = CyclicPartitioner;
module.exports.RandomPartitioner = RandomPartitioner;
module.exports.KeyedPartitioner = KeyedPartitioner;
module.exports.CustomPartitioner = CustomPartitioner;
module.exports.RackawarePartitioner = RackawarePartitioner;
