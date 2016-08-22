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

// [START all]
// [START setup]
// By default, the client will authenticate using the service account file
// specified by the GOOGLE_APPLICATION_CREDENTIALS environment variable and use
// the project specified by the GCLOUD_PROJECT environment variable. See
// https://googlecloudplatform.github.io/gcloud-node/#/docs/google-cloud/latest/guides/authentication
var BigQuery = require('@google-cloud/bigquery');
var Storage = require('@google-cloud/storage');

// Instantiate a bigquery client
var bigquery = BigQuery();
// Instantiate a storage client
var storage = Storage();
// [END setup]

// [START create_table]
/**
 * Creates a new table with the given name in the specified dataset.
 *
 * @param {object} options Configuration options.
 * @param {string} options.dataset The dataset of the new table.
 * @param {string} options.table The name for the new table.
 * @param {string} options.projectId The project ID to use.
 * @param {string|object} [options.schema] The schema for the new table.
 * @param {function} cb The callback function.
 */
function createTable (options, callback) {
  // Instantiate a bigquery client
  var bigquery = BigQuery({
    projectId: options.projectId
  });
  var table = bigquery.dataset(options.dataset).table(options.table);
  var config = {};
  if (options.schema) {
    config.schema = options.schema;
  }

  // See https://googlecloudplatform.github.io/gcloud-node/#/docs/bigquery/latest/bigquery/table
  table.create(config, function (err, table) {
    if (err) {
      return callback(err);
    }

    console.log('Created table: %s', options.table);
    return callback(null, table);
  });
}
// [END create_table]

// [START list_tables]
/**
 * List tables in the specified dataset.
 *
 * @param {object} options Configuration options.
 * @param {string} options.dataset The dataset of the new table.
 * @param {string} options.projectId The project ID to use.
 * @param {Function} callback Callback function.
 */
function listTables (options, callback) {
  // Instantiate a bigquery client
  var bigquery = BigQuery({
    projectId: options.projectId
  });
  var dataset = bigquery.dataset(options.dataset);

  // See https://googlecloudplatform.github.io/gcloud-node/#/docs/bigquery/latest/bigquery/dataset
  dataset.getTables(function (err, tables) {
    if (err) {
      return callback(err);
    }

    console.log('Found %d table(s)!', tables.length);
    return callback(null, tables);
  });
}
// [END list_tables]

// [START delete_table]
/**
 * Creates a new table with the given name in the specified dataset.
 *
 * @param {object} options Configuration options.
 * @param {string} options.dataset The dataset of the new table.
 * @param {string} options.table The name for the new table.
 * @param {string} options.projectId The project ID to use.
 * @param {function} cb The callback function.
 */
function deleteTable (options, callback) {
  // Instantiate a bigquery client
  var bigquery = BigQuery({
    projectId: options.projectId
  });
  var table = bigquery.dataset(options.dataset).table(options.table);

  // See https://googlecloudplatform.github.io/gcloud-node/#/docs/bigquery/latest/bigquery/table
  table.delete(function (err) {
    if (err) {
      return callback(err);
    }

    console.log('Deleted table: %s', options.table);
    return callback(null);
  });
}
// [END delete_table]

// [START import_file]
/**
 * Load a csv file into a BigQuery table.
 *
 * @param {string} file Path to file to load.
 * @param {string} dataset The dataset.
 * @param {string} table The table.
 * @param {Function} callback Callback function.
 */
function importFile (options, callback) {
  var file;
  if (options.bucket) {
    // File is in Google Cloud Storage, e.g. gs://my-bucket/file.csv
    file = storage.bucket(options.bucket).file(options.file);
  } else {
    // File is local, e.g. ./data/file.csv
    file = options.file;
  }
  var table = bigquery.dataset(options.dataset).table(options.table);

  table.import(file, function (err, job) {
    if (err) {
      return callback(err);
    }

    console.log('Started job: %s', job.id);
    job
      .on('error', callback)
      .on('complete', function (metadata) {
        console.log('Completed job: %s', job.id);
        return callback(null, metadata);
      });
  });
}
// [END import_file]
// [END all]

// The command-line program
var cli = require('yargs');

var program = module.exports = {
  createTable: createTable,
  listTables: listTables,
  deleteTable: deleteTable,
  importFile: importFile,
  main: function (args) {
    // Run the command-line program
    cli.help().strict().parse(args).argv;
  }
};

cli
  .demand(1)
  .command('create <dataset> <table>', 'Delete the specified table.', {}, function (options) {
    program.createTable(options, console.log);
  })
  .command('list <datasetId>', 'List tables in the specified dataset.', {}, function (options) {
    program.listTables(options, console.log);
  })
  .command('delete <dataset> <table>', 'Delete the specified table.', {}, function (options) {
    program.deleteTable(options, console.log);
  })
  .command('import <dataset> <table> <file>', 'Create a new bucket with the given name.', {
    bucket: {
      alias: 'b',
      requiresArg: true,
      description: 'Specify Cloud Storage bucket.',
      type: 'string'
    }
  }, function (options) {
    program.importFile(options.bucket, console.log);
  })
  .option('projectId', {
    alias: 'p',
    requiresArg: true,
    type: 'string',
    default: process.env.GCLOUD_PROJECT,
    description: 'Optionally specify the project ID to use.',
    global: true
  })
  .example('node $0 create my_dataset my_table', 'Create table "my_table" in "my_dataset".')
  .example('node $0 list my_dataset', 'List tables in "my-dataset".')
  .example('node $0 delete my_dataset my_table', 'Delete "my_table" from "my_dataset".')
  .example('node $0 import my_dataset my_table ./data.csv', 'Import a local file into a table.')
  .example('node $0 import my_dataset my_table data.csv --bucket my-bucket', 'Import a GCS file into a table.')
  .wrap(80)
  .recommendCommands()
  .epilogue('For more information, see https://cloud.google.com/bigquery/docs');

if (module === require.main) {
  program.main(process.argv.slice(2));
}
