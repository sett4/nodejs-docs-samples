// Copyright 2016, Google, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

var BigQuery = require('@google-cloud/bigquery');
var Storage = require('@google-cloud/storage');
var uuid = require('node-uuid');
var program = require('../_tables');
var path = require('path');

var bigquery = BigQuery();
var storage = Storage();
var projectId = process.env.GCLOUD_PROJECT;
var localFilePath = path.join(__dirname, '../resources/data.csv');
var bucketName = 'nodejs-docs-samples-test-' + uuid.v4();
var datasetId = 'nodejs-docs-samples-test-' + uuid.v4();
var tableId = 'nodejs-docs-samples-test-' + uuid.v4();

// BigQuery only accepts underscores
datasetId = datasetId.replace(/-/gi, '_');
tableId = tableId.replace(/-/gi, '_');

describe.only('bigquery:tables', function () {
  before(function (done) {
    storage.createBucket(bucketName, function (err, bucket) {
      if (err) {
        return done(err);
      }
      bucket.upload(localFilePath, function (err) {
        if (err) {
          return done(err);
        }

        bigquery.createDataset(datasetId, done);
      });
    });
  });

  after(function (done) {
    bigquery.dataset(datasetId).delete({
      force: true
    }, function () {
      // Ignore any error, the dataset might already have been successfully deleted
      done();
      storage.bucket(bucketName).deleteFiles({ force: true }, function (err) {
        if (err) {
          return done(err);
        }
        storage.bucket(bucketName).delete(done);
      });
    });
  });

  describe('createTable', function () {
    it('should create a new table', function (done) {
      var options = {
        projectId: projectId,
        dataset: datasetId,
        table: tableId
      };

      program.createTable(options, function (err, table) {
        assert.ifError(err);
        assert(table, 'new table was created');
        assert.equal(table.id, tableId);
        assert(console.log.calledWith('Created table: %s', tableId));
        done();
      });
    });
  });

  describe('listTables', function () {
    it('should list tables', function (done) {
      var options = {
        projectId: projectId,
        dataset: datasetId
      };

      program.listTables(options, function (err, tables) {
        assert.ifError(err);
        assert(Array.isArray(tables));
        assert(tables.length > 0);
        assert(tables[0].id);
        var matchingTables = tables.filter(function (table) {
          return table.id === tableId;
        });
        assert.equal(matchingTables.length, 1, 'newly created table is in list');
        assert(console.log.calledWith('Found %d table(s)!', tables.length));
        done();
      });
    });
  });

  describe('deleteTable', function () {
    it('should list tables', function (done) {
      var options = {
        projectId: projectId,
        dataset: datasetId,
        table: tableId
      };

      program.deleteTable(options, function (err) {
        assert.ifError(err);
        assert(console.log.calledWith('Deleted table: %s', tableId));
        done();
      });
    });
  });
});
