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

// [START complete]
/**
 * Command-line application to export a table from BigQuery to Google Cloud Storage.
 *
 * This sample is used on this page:
 *
 *   https://cloud.google.com/bigquery/exporting-data-from-bigquery
 * For more information, see the README.md under /bigquery.
 */

'use strict';

// [START auth]
// By default, gcloud will authenticate using the service account file specified
// by the GOOGLE_APPLICATION_CREDENTIALS environment variable and use the
// project specified by the GCLOUD_PROJECT environment variable. See
// https://googlecloudplatform.github.io/gcloud-node/#/docs/guides/authentication
var BigQuery = require('@google-cloud/bigquery');
var Storage = require('@google-cloud/storage');
var projectId = process.env.GCLOUD_PROJECT;

// Instantiate the BigQuery and Storage clients
var bigquery = BigQuery();
var storage = Storage({
  projectId: projectId
});
// [END auth]

// [START export_table_to_gcs]
/**
 * Export a table from BigQuery to Google Cloud Storage.
 *
 * @param {object} options Configuration options.
 * @param {string} options.bucket A Google Cloud Storage bucket to use for storage.
 * @param {string} options.file The file to save results to within Google Cloud Storage.
 * @param {string} options.dataset The ID of the dataset to use.
 * @param {string} options.table The ID of the project to use.
 * @param {string} options.format Format to export as - either 'CSV', 'JSON', or 'AVRO'.
 * @param {boolean} [options.gzip] Optional. Whether or not data should be compressed using GZIP.
 * @param {function} callback Callback function to receive query results.
 */
function exportTableToGCS (options, callback) {
  var gcsFileObj = storage.bucket(options.bucket).file(options.file);

  // Export table
  // See https://googlecloudplatform.github.io/gcloud-node/#/docs/google-cloud/latest/bigquery/table?method=export
  var table = bigquery.dataset(options.dataset).table(options.table);
  table.export(
    gcsFileObj,
    { format: options.format, gzip: options.gzip },
    function (err, job) {
      if (err) {
        return callback(err);
      }

      console.log('ExportTableToGCS: submitted job %s!', job.id);
      return callback(null, job);
    }
  );
}
// [END export_table_to_gcs]

// [START poll_export_job]
/**
 * Check the status of a BigQuery table export job.
 *
 * @param {string} jobId The ID of the export job to poll.
 * @param {function} callback Callback function to execute when the table is exported.
 */
function pollExportJob (jobId, callback) {
  var job = bigquery.job(jobId);
  job.getMetadata(function (err, metadata) {
    if (err) {
      return callback(err);
    }
    console.log('PollExportJob: job status: %s', metadata.status.state);

    // If job is done, return metadata; if not, return an error.
    if (metadata.status.state === 'DONE') {
      return callback(null, metadata);
    } else {
      return callback(new Error('Job %s is not done', jobId));
    }
  });
}
// [END poll_export_job]
// [END complete]

// The command-line program
var cli = require('yargs');

var program = module.exports = {
  exportTableToGCS: exportTableToGCS,
  pollExportJob: pollExportJob,
  main: function (args) {
    // Run the command-line program
    cli.help().strict().parse(args).argv;
  }
};

cli
  .command('export <bucket> <file> <dataset> <table> [--format] [--gzip]', 'Export a table from BigQuery to Google Cloud Storage.', {}, function (options) {
    program.exportTableToGCS(options, console.log);
  })
  .command('poll <jobId>', 'Check the status of a BigQuery table export job.', {}, function (options) {
    program.pollExportJob(options.jobId, console.log);
  })
  .example('node $0 export sample-bigquery-export data.json github_samples natality JSON', 'Export github_samples:natality to gcs://sample-bigquery-export/data.json as JSON')
  .example('node $0 poll job_12345ABCDE', 'Check the status of BigQuery job 12345ABCDE')
  .options({
    format: {
      alias: 'f',
      global: true,
      requiresArg: true,
      type: 'string',
      choices: ['JSON', 'CSV', 'AVRO']
    },
    gzip: {
      global: true,
      type: 'boolean',
      description: 'Whether to compress the exported table using gzip. Defaults to false.'
    }
  })
  .wrap(100)
  .recommendCommands()
  .epilogue('For more information, see https://cloud.google.com/bigquery/exporting-data-from-bigquery');

if (module === require.main) {
  program.main(process.argv.slice(2), console.log);
}

module.exports = program;
