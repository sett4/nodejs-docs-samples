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

var proxyquire = require('proxyquire').noCallThru();
var datasetId = 'foo';
var projectId = process.env.GCLOUD_PROJECT;

function getSample () {
  var tableMock = {
    get: sinon.stub(),
    metadata: {
      numBytes: 1000000
    }
  };
  tableMock.get.callsArgWith(0, null, tableMock);
  var tablesMock = [
    tableMock
  ];
  var datasetsMock = [
    {
      id: datasetId
    }
  ];
  var datasetMock = {
    getTables: sinon.stub().callsArgWith(0, null, tablesMock),
    create: sinon.stub().callsArgWith(0, null, datasetsMock[0]),
    delete: sinon.stub().callsArgWith(0, null)
  };
  var bigqueryMock = {
    getDatasets: sinon.stub().callsArgWith(0, null, datasetsMock),
    dataset: sinon.stub().returns(datasetMock)
  };
  var BigQueryMock = sinon.stub().returns(bigqueryMock);

  return {
    program: proxyquire('../datasets', {
      '@google-cloud/bigquery': BigQueryMock
    }),
    mocks: {
      BigQuery: BigQueryMock,
      bigquery: bigqueryMock,
      datasets: datasetsMock,
      dataset: datasetMock,
      tables: tablesMock,
      table: tableMock
    }
  };
}

describe('bigquery:datasets', function () {
  describe('createDataset', function () {
    it('should create a dataset', function () {
      var sample = getSample();
      var callback = sinon.stub();

      sample.program.createDataset(datasetId, projectId, callback);

      assert(sample.mocks.dataset.create.calledOnce, 'method called once');
      assert.equal(sample.mocks.dataset.create.firstCall.args.length, 1, 'method received 1 argument');
      assert(callback.calledOnce, 'callback called once');
      assert.equal(callback.firstCall.args.length, 2, 'callback received 2 arguments');
      assert.ifError(callback.firstCall.args[0], 'callback did not receive error');
      assert.strictEqual(callback.firstCall.args[1], sample.mocks.datasets[0], 'callback received result');
      assert(console.log.calledWith('Created dataset: %s', datasetId));
    });

    it('should handle error', function () {
      var error = 'error';
      var sample = getSample();
      var callback = sinon.stub();
      sample.mocks.dataset.create = sinon.stub().callsArgWith(0, error);

      sample.program.createDataset(datasetId, projectId, callback);

      assert(callback.calledOnce, 'callback called once');
      assert.equal(callback.firstCall.args.length, 1, 'callback received 1 argument');
      assert(callback.firstCall.args[0], 'callback received error');
      assert.equal(callback.firstCall.args[0].message, error.message, 'error has correct message');
    });
  });

  describe('deleteDataset', function () {
    it('should delete a dataset', function () {
      var sample = getSample();
      var callback = sinon.stub();

      sample.program.deleteDataset(datasetId, projectId, callback);

      assert(sample.mocks.dataset.delete.calledOnce, 'method called once');
      assert.equal(sample.mocks.dataset.delete.firstCall.args.length, 1, 'method received 1 argument');
      assert(callback.calledOnce, 'callback called once');
      assert.equal(callback.firstCall.args.length, 1, 'callback received 1 argument');
      assert.ifError(callback.firstCall.args[0], 'callback did not receive error');
      assert(console.log.calledWith('Deleted dataset: %s', datasetId));
    });

    it('should handle error', function () {
      var error = 'error';
      var sample = getSample();
      var callback = sinon.stub();
      sample.mocks.dataset.delete = sinon.stub().callsArgWith(0, error);

      sample.program.deleteDataset(datasetId, projectId, callback);

      assert(callback.calledOnce, 'callback called once');
      assert.equal(callback.firstCall.args.length, 1, 'callback received 1 argument');
      assert(callback.firstCall.args[0], 'callback received error');
      assert.equal(callback.firstCall.args[0].message, error.message, 'error has correct message');
    });
  });

  describe('listDatasets', function () {
    it('should list datasets', function () {
      var sample = getSample();
      var callback = sinon.stub();

      sample.program.listDatasets(projectId, callback);

      assert(sample.mocks.bigquery.getDatasets.calledOnce, 'method called once');
      assert.equal(sample.mocks.bigquery.getDatasets.firstCall.args.length, 1, 'method received 1 argument');
      assert(callback.calledOnce, 'callback called once');
      assert.equal(callback.firstCall.args.length, 2, 'callback received 2 arguments');
      assert.ifError(callback.firstCall.args[0], 'callback did not receive error');
      assert.strictEqual(callback.firstCall.args[1], sample.mocks.datasets, 'callback received result');
      assert(console.log.calledWith('Found %d dataset(s)!', sample.mocks.datasets.length));
    });

    it('should handle error', function () {
      var error = 'error';
      var sample = getSample();
      var callback = sinon.stub();
      sample.mocks.bigquery.getDatasets = sinon.stub().callsArgWith(0, error);

      sample.program.listDatasets(projectId, callback);

      assert(callback.calledOnce, 'callback called once');
      assert.equal(callback.firstCall.args.length, 1, 'callback received 1 argument');
      assert(callback.firstCall.args[0], 'callback received error');
      assert.equal(callback.firstCall.args[0].message, error.message, 'error has correct message');
    });
  });

  describe('getDatasetSize', function () {
    it('should calculate size of a dataset', function () {
      var sample = getSample();
      var callback = sinon.stub();

      sample.program.getDatasetSize(datasetId, projectId, callback);

      assert(sample.mocks.dataset.getTables.calledOnce, 'method called once');
      assert.equal(sample.mocks.dataset.getTables.firstCall.args.length, 1, 'method received 1 argument');
      assert(callback.calledOnce, 'callback called once');
      assert.equal(callback.firstCall.args.length, 2, 'callback received 2 arguments');
      assert.ifError(callback.firstCall.args[0], 'callback did not receive error');
      assert.strictEqual(callback.firstCall.args[1], 1, 'callback received result');
      assert(console.log.calledWith('Size of %s: %d MB', datasetId, 1));
    });

    it('should handle dataset.getTables error', function () {
      var error = 'error';
      var sample = getSample();
      var callback = sinon.stub();
      sample.mocks.dataset.getTables = sinon.stub().callsArgWith(0, error);

      sample.program.getDatasetSize(datasetId, projectId, callback);

      assert(callback.calledOnce, 'callback called once');
      assert.equal(callback.firstCall.args.length, 1, 'callback received 1 argument');
      assert(callback.firstCall.args[0], 'callback received error');
      assert.equal(callback.firstCall.args[0].message, error.message, 'error has correct message');
    });

    it('should handle table.get error', function () {
      var error = 'error';
      var sample = getSample();
      var callback = sinon.stub();
      sample.mocks.table.get = sinon.stub().callsArgWith(0, error);

      sample.program.getDatasetSize(datasetId, projectId, callback);

      assert(callback.calledOnce, 'callback called once');
      assert.equal(callback.firstCall.args.length, 1, 'callback received 1 argument');
      assert(callback.firstCall.args[0], 'callback received error');
      assert.equal(callback.firstCall.args[0].message, error.message, 'error has correct message');
    });
  });

  describe('main', function () {
    it('should call createDataset', function () {
      var program = getSample().program;

      sinon.stub(program, 'createDataset');
      program.main(['create', datasetId]);
      assert(program.createDataset.calledOnce);
    });

    it('should call deleteDataset', function () {
      var program = getSample().program;

      sinon.stub(program, 'deleteDataset');
      program.main(['delete', datasetId]);
      assert(program.deleteDataset.calledOnce);
    });

    it('should call listDatasets', function () {
      var program = getSample().program;

      sinon.stub(program, 'listDatasets');
      program.main(['list']);
      assert(program.listDatasets.calledOnce);
    });

    it('should call getDatasetSize', function () {
      var program = getSample().program;

      sinon.stub(program, 'getDatasetSize');
      program.main(['size', datasetId]);
      assert(program.getDatasetSize.calledOnce);
    });
  });
});
