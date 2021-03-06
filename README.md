# GA4GH Workflow Interoperability

> **This repo has been transferred to [ga4gh/cloud-interop-testing](https://github.com/ga4gh/cloud-interop-testing) for future development and maintainence as the testbed scope expands. The current repo will be left up reference/archival purposes only.**

[![Travis-CI Build Status](https://travis-ci.org/Sage-Bionetworks/workflow-interop.svg?branch=develop)](https://travis-ci.org/Sage-Bionetworks/workflow-interop.svg?branch=develop) 
[![Coverage Status](https://coveralls.io/repos/github/Sage-Bionetworks/workflow-interop/badge.svg?branch=develop)](https://coveralls.io/Sage-Bionetworks/workflow-interop?branch=develop)


The initial use case for this app will be to act as a workflow orchestrator and bridge between **Tool Registry Service (TRS)** and **Workflow Execution Service (WES)** endpoints for the [**Testbed Interoperability Platform**](https://docs.google.com/document/d/12Mq4v7o5VKF-DkFTQwsUQ-aWZ5aBeIcl_5YrhbaSv7M/edit?usp=sharing), a core deliverable of the GA4GH Cloud Workstream for 2018.

## Overview

In the context of the testbed, the orchestrator performs 3 primary tasks:

1. Look up a workflow registered in a TRS implementation, identify its corresponding "checker" workflow, and retrieve any data required to run the checker workflow;
2. Format checker workflow data and initiate new workflow runs on one or more WES endpoints;
3. Reports results.

Additionally, the application supports the following operations:

+ Register and configure new TRS endpoints;
+ Register and configure new WES endpoints;
+ Onboard/register a new workflow (by creating and configuring a queue with workflow details).

## Installation

```console
git clone https://github.com/Sage-Bionetworks/workflow-interop
pip install .
pip install toil[all] challengeutils chevron
```


## Usage

*CLI should be available soon.*

### Load modules

These modules should cover most of the current use cases:

```python
from wfinterop import config
from wfinterop import orchestrator
from wfinterop import testbed
```

### Default settings

```python
config.show()
```

```console
Orchestrator options:

Workflow Evaluation Queues
(queue ID: workflow ID [version])
---------------------------------------------------------------------------
test_wdl_queue: None (None)
  > workflow URL: file://tests/testdata/md5sum.wdl
  > workflow attachments:
    - file://tests/testdata/md5sum.input
  > workflow type: CWL
  > from TRS: None
  > WES options: ['local']
test_cwl_queue: None (None)
  > workflow URL: file://tests/testdata/md5sum.cwl
  > workflow attachments:
    - file://tests/testdata/md5sum.input
    - file://tests/testdata/dockstore-tool-md5sum.cwl
  > workflow type: CWL
  > from TRS: None
  > WES options: ['local']

Tool Registries
(TRS ID: host address)
---------------------------------------------------------------------------
dockstore: dockstore.org:8443

Workflow Services
(WES ID: host address)
---------------------------------------------------------------------------
local: 0.0.0.0:8080
```

<details>

<summary>View YAML</summary>

```yaml
queues:
  test_cwl_queue:
    target_queue: null
    trs_id: null
    version_id: null
    wes_default: local
    wes_opts:
    - local
    workflow_attachments:
    - file://tests/testdata/md5sum.input
    - file://tests/testdata/dockstore-tool-md5sum.cwl
    workflow_id: null
    workflow_type: CWL
    workflow_url: file://tests/testdata/md5sum.cwl
  test_wdl_queue:
    target_queue: null
    trs_id: null
    version_id: null
    wes_default: local
    wes_opts:
    - local
    workflow_attachments:
    - file://tests/testdata/md5sum.input
    workflow_id: null
    workflow_type: CWL
    workflow_url: file://tests/testdata/md5sum.wdl
toolregistries:
  dockstore:
    auth:
      Authorization: ''
    host: dockstore.org:8443
    proto: https
workflowservices:
  local:
    auth:
      Authorization: ''
    host: 0.0.0.0:8080
    proto: http
```

</details>

### Add a workflow

```python
config.add_queue(queue_id='demo_queue',
                 wf_type='CWL',
                 wf_id='github.com/dockstore-testing/md5sum-checker',
                 version_id='develop',
                 trs_id='dockstore')
```

```console
Workflow Evaluation Queues
(queue ID: workflow ID [version])
---------------------------------------------------------------------------
test_wdl_queue: None (None)
  > workflow URL: file://tests/testdata/md5sum.wdl
  > workflow type: CWL
  > from TRS: None
  > WES options: ['local']
test_cwl_queue: None (None)
  > workflow URL: file://tests/testdata/md5sum.cwl
  > workflow type: CWL
  > from TRS: None
  > WES options: ['local']
demo_queue: github.com/dockstore-testing/md5sum-checker (develop)
  > workflow URL: None
  > workflow type: CWL
  > from TRS: dockstore
  > WES options: ['local']

...
```

<details>

<summary>View YAML</summary>

```yaml
demo_queue:
  target_queue: null
  trs_id: dockstore
  version_id: develop
  wes_default: local
  wes_opts:
  - local
  workflow_attachments: null
  workflow_id: github.com/dockstore-testing/md5sum-checker
  workflow_type: CWL
  workflow_url: null
```

</details>

### Add a WES endpoint

```python
config.add_workflowservice(service='arvados-wes',
                           host='wes.qr1hi.arvadosapi.com',
                           auth={'Authorization': 'Bearer <my-api-token>'},
                           proto='https')
```

```console
Workflow Services
(WES ID: host address)
---------------------------------------------------------------------------
arvados-wes: wes.qr1hi.arvadosapi.com
local: 0.0.0.0:8080
```

#### Connect a WES endpoint to a workflow queue

```python
config.add_wes_opt(queue_ids='demo_queue', wes_id='arvados-wes')
```

```console
Workflow Evaluation Queues
(queue ID: workflow ID [version])
---------------------------------------------------------------------------
queue_2: github.com/dockstore-testing/md5sum-checker (develop)
  > workflow URL: None
  > workflow type: CWL
  > from TRS: dockstore
  > WES options: ['local', 'arvados-wes']
```

### Running workflows

#### Setting up a local WES service

This package uses (and installs as a dependency) the [**workflow-service** package](https://github.com/common-workflow-language/workflow-service), which provides both client and server implementations of the WES API.

You can start service running `cwltool` by running this command in the terminal:

```console
wes-server
```

You should see a message that looks something like this:

```console
INFO:root:Using config:
INFO:root:  opt: None
INFO:root:  debug: False
INFO:root:  version: False
INFO:root:  port: 8080
INFO:root:  backend: wes_service.cwl_runner
 * Serving Flask app "wes_service.wes_service_main" (lazy loading)
 * Environment: production
   WARNING: Do not use the development server in a production environment.
   Use a production WSGI server instead.
 * Debug mode: off
INFO:werkzeug: * Running on http://0.0.0.0:8080/ (Press CTRL+C to quit)
```

> Note: running WDL workflows using a local service has not been fully tested.

#### Monitoring orchestrator activity

In a seperate terminal window or notebook, you can start a `monitor` process to keep track of any active workflow jobs.

```python
orchestrator.monitor()
```
#### Check a workflow

To check a workflow in the testbed in a single environment...

```python
testbed.check_workflow(queue_id='demo_queue', wes_id='local')
```

To check combinations of workflows and environments...

```python
testbed.check_all({'demo_queue': ['local', 'arvados-wes']})
```

#### Run a workflow job

To run a workflow using a given set of parameters...

```python
orchestrator.run_job(queue_id='test_cwl_queue',
                     wes_id='local',
                     wf_jsonyaml='file://tests/testdata/md5sum.cwl.json')
```


#### Synapse Orchestration

Configuring a queue in `queues.yaml`.  Replace 12345 with your Synapse Evaluation id.

```yaml
12345:
  target_queue: null
  trs_id: null
  version_id: null
  wes_default: local
  wes_opts:
  - local
  workflow_attachments:
  - file://tests/input.cwl
  workflow_id: null
  workflow_type: CWL
  workflow_url: /path/to/workflow.cwl
```

Running submissions from Synapse

```python
from wfinterop import synapse_orchestrator
import synapseclient
syn = synapseclient.login()

# Workflow inputs
synapse_orchestrator.run_submission(syn, queue_id=9614486,
                                    submission_id=9703501,
                                    wes_id='local')

synapse_orchestrator.run_queue(syn, queue_id=9614486,
                               wes_id='local')
synapse_orchestrator.monitor_queue(syn, queue_id=9614486)

# Docker submissions
synapse_orchestrator.run_submission(syn, queue_id=9614487,
                                    submission_id=9703500,
                                    wes_id='local')

synapse_orchestrator.run_queue(syn, queue_id=9614487,
                               wes_id='local')
synapse_orchestrator.monitor_queue(syn, queue_id=9614487)

# Workflow submissions
synapse_orchestrator.run_submission(syn, queue_id=9614488,
                                    submission_id=9703508,
                                    wes_id='local')

synapse_orchestrator.run_queue(syn, queue_id=9614488,
                               wes_id='local')
synapse_orchestrator.monitor_queue(syn, queue_id=9614488)


# Prediction file
synapse_orchestrator.run_submission(syn, queue_id=9614489,
                                    submission_id=9703603,
                                    wes_id='local')

synapse_orchestrator.run_queue(syn, queue_id=9614489,
                               wes_id='local')
synapse_orchestrator.monitor_queue(syn, queue_id=9614489)


```

