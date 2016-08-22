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

function getSample () {
  var projectsMock = [
    {
      id: 'foo',
      name: 'foo'
    }
  ];
  var resourceMock = {
    getProjects: sinon.stub().callsArgWith(0, null, projectsMock, null, projectsMock)
  };
  var ResourceMock = sinon.stub().returns(resourceMock);

  return {
    program: proxyquire('../projects', {
      '@google-cloud/resource': ResourceMock,
      yargs: proxyquire('yargs', {})
    }),
    mocks: {
      Resource: ResourceMock,
      resource: resourceMock,
      projects: projectsMock
    }
  };
}

describe('resource:projects', function () {
  describe('listProjects', function () {
    it('should list projects', function () {
      var sample = getSample();
      var callback = sinon.stub();

      sample.program.listProjects(callback);

      assert(sample.mocks.resource.getProjects.calledOnce, 'method called once');
      assert.equal(sample.mocks.resource.getProjects.firstCall.args.length, 1, 'method received 1 argument');
      assert(callback.calledOnce, 'callback called once');
      assert.equal(callback.firstCall.args.length, 2, 'callback received 2 arguments');
      assert.ifError(callback.firstCall.args[0], 'callback did not receive error');
      assert.strictEqual(callback.firstCall.args[1], sample.mocks.projects, 'callback received result');
      assert(console.log.calledWith('Found %d project(s)!', sample.mocks.projects.length));
    });

    it('should handle error', function () {
      var error = 'error';
      var sample = getSample();
      var callback = sinon.stub();
      sample.mocks.resource.getProjects = sinon.stub().callsArgWith(0, error);

      sample.program.listProjects(callback);

      assert(callback.calledOnce, 'callback called once');
      assert.equal(callback.firstCall.args.length, 1, 'callback received 1 argument');
      assert(callback.firstCall.args[0], 'callback received error');
      assert.equal(callback.firstCall.args[0].message, error.message, 'error has correct message');
    });
  });

  describe('main', function () {
    it('should call listProjects', function () {
      var program = getSample().program;

      sinon.stub(program, 'listProjects');
      program.main(['list']);
      assert(program.listProjects.calledOnce);
    });
  });
});
