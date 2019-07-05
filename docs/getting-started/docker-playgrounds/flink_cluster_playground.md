---
title: "Flink Cluster Playground"
nav-title: 'Flink Cluster Playground'
nav-parent_id: docker-playgrounds
nav-pos: 1
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

There are many ways to deploy and operate Apache Flink in various environments. Regardless of this
variety, the fundamental building blocks of a Flink Cluster remain the same and similar
operational principles apply.

This docker compose-based playground will get you started with Apache Flink operations quickly and
will briefly introduce you to the main components that make up a Flink Cluster.

* This will be replaced by the TOC
{:toc}

## Anatomy of this Playground

This playground consists of a long living
[Flink Session Cluster]({{ site.baseurl }}/concepts/glossary.html#flink-session-cluster) and a Kafka
Cluster.

A Flink Cluster always consists of a 
[Flink Master]({{ site.baseurl }}/concepts/glossary.html#flink-master) and one or more 
[Flink TaskManagers]({{ site.baseurl }}/concepts/glossary.html#flink-taskmanager). The Flink Master 
is responsible to handle Job submissions, the supervision of Jobs as well as resource 
management. The Flink TaskManagers are the worker processes and are responsible for the execution of 
the actual [Tasks]({{ site.baseurl }}/concepts/glossary.html#task) which make up a Flink Job. In 
this playground you will start with a single TaskManager, but scale out to more TaskManagers later. 
Additionally, this playground comes with a dedicated *client* container, which we use to submit the 
Flink Job initially and to perform various operational tasks later on.

The Kafka Cluster consists of a Zookeeper server and a Kafka Broker.

<img src="{{ site.baseurl }}/fig/flink-docker-playground.svg" alt="Flink Docker Playground"
class="offset" width="80%" />

When the playground is started a Flink Job called *Flink Event Count* will be submitted to the 
Flink Master. Additionally, two Kafka Topics *input* and *output* are created.

<img src="{{ site.baseurl }}/fig/click-event-count-example.svg" alt="Click Event Count Example"
class="offset" width="80%" />

The Job consumes `ClickEvent`s from the *input* topic, each with a `timestamp` and a `page`. The 
events are then keyed by `page` and counted in one minute 
[windows]({{ site.baseurl }}/dev/stream/operators/windows.html). The results are written to the 
*output* topic. 

There are six different `page`s and the **events are generated so that each window contains exactly 
one thousand records**.

{% top %}

## Setup

{% if site.version contains "SNAPSHOT" %}
<p style="border-radius: 5px; padding: 5px" class="bg-danger">
  <b>Note</b>: The Apache Flink Docker images used for this playground are only available for
  released versions of Apache Flink. Since you are currently looking at the latest SNAPSHOT
  version of the documentation the branch referenced below will not exist. You can either change it 
  manually or switch to the released version of the ocumentation via the release picker.
</p>
{% endif %}

In this section you will setup the playground locally on your machine and verify that the Job is 
running successfully.

This guide assumes that you have [docker](https://docs.docker.com/) (1.12+) and
[docker-compose](https://docs.docker.com/compose/) (2.1+) installed on your machine.

The required configuration files are available in the 
[flink-playgrounds](https://github.com/apache/flink-playgrounds) repository. Check it out and spin
up the environment:

{% highlight bash %}
git clone --branch release-{{ site.version }} git@github.com:apache/flink-playgrounds.git
cd flink-cluster-playground
docker-compose up -d
{% endhighlight %}

Afterwards, `docker-compose ps` should give you the following output:

{% highlight bash %}
                     Name                                    Command               State                   Ports                
--------------------------------------------------------------------------------------------------------------------------------
flink-cluster-playground_clickevent-generator_1   /docker-entrypoint.sh java ...   Up       6123/tcp, 8081/tcp                  
flink-cluster-playground_client_1                 /docker-entrypoint.sh flin ...   Exit 0                                       
flink-cluster-playground_jobmanager_1             /docker-entrypoint.sh jobm ...   Up       6123/tcp, 0.0.0.0:8081->8081/tcp    
flink-cluster-playground_kafka_1                  start-kafka.sh                   Up       0.0.0.0:9094->9094/tcp              
flink-cluster-playground_taskmanager_1            /docker-entrypoint.sh task ...   Up       6123/tcp, 8081/tcp                  
flink-cluster-playground_zookeeper_1              /bin/sh -c /usr/sbin/sshd  ...   Up       2181/tcp, 22/tcp, 2888/tcp, 3888/tcp
{% endhighlight %}

This indicates that the client container has successfully submitted the Flink Job ("Exit 0") and all 
cluster components as well as the data generator are running ("Up").

## Interaction with the Playground

There are many ways to interact with this playground and its components.

### Flink WebUI

The Flink WebUI is probably the most natural starting point to observe your Flink Cluster. It is
exposed under `http://localhost:8081`. If everything went well, you'll see that the cluster initially 
consists of one TaskManager and one Job called *Click Event Count* is in "RUNNING" state.

<img src="{{ site.baseurl }}/fig/playground-webui.png" alt="Playground Flink WebUI"
class="offset" width="100%" />

### Logs

**JobManager**

The JobManager logs can be tailed via `docker-compose`.

{% highlight bash %}
docker-compose logs -f jobmanager
{% endhighlight %}

After the initial startup you should mainly see log messages for every checkpoint completion.

**TaskManager**

The TaskManager log can be tailed in the same way.
{% highlight bash %}
docker-compose logs -f taskmanager
{% endhighlight %}

After the initial startup you should mainly see log messages for every checkpoint completion.

### Flink CLI

The [Flink CLI]({{ site.baseurl }}/ops/cli.html) can be used from within the client container. For
example, to print the `help` message of the Flink CLI you can run
{% highlight bash%}
docker-compose run --no-deps client flink --help
{% endhighlight %}

### Flink REST API

The [Flink REST API]({{ site.baseurl }}/monitoring/rest_api.html#api) is exposed via
`localhost:8081` on the host or via `jobmanager:8081` from the client container, e.g. to list all
currently running jobs, you can run:
{% highlight bash%}
docker-compose run --no-deps client curl jobmanager:8081/jobs
{% endhighlight %}

### Kafka Topics

To manually look at the records in the Kakfa Topics, you can run
{% highlight bash%}
//input topic (1000 records/s)
docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic input
//output topic (6 records/min)
docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output
{% endhighlight %}

{%  top %}

## Tear Down

To tear down the environment (including the removal of all volumes) run:
{% highlight bash%}
docker-compose down -v
{% endhighlight %}

{%  top %}

## Operational Plays

This section describes some prototypical operational activities in the context of this playground. 
They do not need to be executed in any particular order. Most of these tasks can be performed either
via the [CLI](#flink-cli) or the [REST API](#flink-rest-api).

### Listing Running Jobs

<div class="codetabs" markdown="1">
<div data-lang="CLI" markdown="1">
**Command**
{% highlight bash %}
docker-compose run --no-deps client flink list
{% endhighlight %}
**Expected Output**
{% highlight plain %}
Waiting for response...
------------------ Running/Restarting Jobs -------------------
16.07.2019 16:37:55 : <job-id> : Click Event Count (RUNNING)
--------------------------------------------------------------
No scheduled jobs.
{% endhighlight %}
</div>
<div data-lang="REST API" markdown="1">
**Request**
{% highlight bash %}
docker-compose run --no-deps client curl jobmanager:8081/jobs
{% endhighlight %}
**Expected Response (pretty-printed)**
{% highlight bash %}
{
  "jobs": [
    {
      "id": "<job-id>",
      "status": "RUNNING"
    }
  ]
}
{% endhighlight %}
</div>
</div>

### Observing Failure & Recovery

Flink provides exactly-once processing guarantees under (partial) failure. In this playground you 
can observe and - to some extent - verify this behavior. 

#### Step 1: Observing the Output

As described [above](#anatomy-of-this-playground) the events in this playground are generate such 
that each one minute window  contains exactly one thousand records. So, in order to verify that 
Flink successfully recovers from a TaskManager failure without data loss or duplication you can tail 
the output topic and check that - after recovery - all windows are present and the count is correct.

For this, start reading from the *output* topic and leave this command running until after 
recovery (Step 3).

{% highlight bash%}
docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output
{% endhighlight %}

#### Step 2: Introducing a Fault

In order to simulate a partial failure you can kill a TaskManager. In a production setup, this 
could correspond to a loss of the TaskManager process, the TaskManager machine or simply a transient 
exception being thrown from the framework or user code (e.g. due to the temporary unavailability of 
an external resource).   

{% highlight bash%}
docker-compose kill taskmanager
{% endhighlight %}

After a few seconds, you will see in the Flink WebUI that the Job failed, and has been 
automatically resubmitted. At this point, it can not be restarted though due to the lack of 
resources (no TaskSlots provided by TaskManagers) and will go through a cycle of cancellations and 
resubmissions until resources become available again.

<img src="{{ site.baseurl }}/fig/playground-webui-failure.png" alt="Playground Flink WebUI" 
class="offset" width="100%" />

In the meantime, the data generator keeps pushing `ClickEvent`s into the *input* topic. 

#### Step 3: Recovery

Once you restart the TaskManager the Job will recover from its last successful 
[checkpoint]({{ site.baseurl }}/internals/stream_checkpointing.html) prior to the failure.

{% highlight bash%}
docker-compose up -d taskmanager
{% endhighlight %}

Once the new TaskManager has registered itself with the Flink Master, the Job will start "RUNNING" 
again. It will then quickly process the full backlog you can of input events from Kafka and produce output 
at a much higher rate (> 6 records/minute) until it has caught up to the head of the queue. In the 
*output* you will see that all keys (`page`s) are present for all time windows and the count is 
exactly one thousand.

### Upgrading & Rescaling the Job

Upgrading a Flink Job always involves two steps: First, the Flink Job is gracefully stopped with a
[Savepoint]({{site.base_url}}/ops/state/savepoints.html). A Savepoint is a consistent snapshot of 
the complete application state at well-defined, globally consistent point in time (similar to a 
checkpoint). Second, the upgraded Flink Job is started from the Savepoint. Inthis context "upgrade" 
can mean different things including the following:

* An upgrade to the configuration (incl. the parallelism of the Job)
* An upgrade to the topology of the Job
* An upgrade to the user-defined functions of the Job

Before starting with the upgrade you might want to start tailing the *output* topic, in order to 
observe that no data is lost or corrupted in the course the upgrade. 

{% highlight bash%}
docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output
{% endhighlight %}

#### Step 1: Stopping the Job

To gracefully stop the Job, you need to use the "stop" command of either the CLI or the REST API. 
For this you will need the JobID of the Job, which is you can obtain by 
[listing all running Jobs](#listing-running-jobs). With the JobID you can proceed to stopping the 
Job:

<div class="codetabs" markdown="1">
<div data-lang="CLI" markdown="1">
**Command**
{% highlight bash %}
docker-compose run --no-deps client flink stop <job-id>
{% endhighlight %}
**Expected Output**
{% highlight bash %}
Suspending job "<job-id>" with a savepoint.
Suspended job "<job-id>" with a savepoint.
{% endhighlight %}
</div>
 <div data-lang="REST API" markdown="1">
 
 **Request**
{% highlight bash %}
# triggering stop
docker-compose run --no-deps client curl -X POST jobmanager:8081/jobs/<job-id>/stop -d '{"drain": false}'
{% endhighlight %}

**Expected Response (pretty-printed)**
{% highlight json %}
{
  "request-id": "<trigger-id>"
}
{% endhighlight %}

**Request**
{% highlight bash %}
# check status of stop action
 docker-compose run --no-deps client curl jobmanager:8081/jobs/<job-id>/savepoints/<trigger-id>
{% endhighlight %}

**Expected Response (pretty-printed)**
{% highlight json %}
{
  "status": {
    "id": "COMPLETED"
  },
  "operation": {
    "location": "<savepoint-path>"
  }

{% endhighlight %}
</div>
</div>

The Savepoint has been stored to the `state.savepoint.dir` configured in the *flink-conf.yaml*, 
which is mounted under */tmp/flink-savepoints-directory/* on your local machine. You will need the 
path to this Savepoint in the next step. In case of the REST API this path was already part of the 
response, you will need to have a look at the filesystem directly.

**Command**
{% highlight bash %}
ls -lia /tmp/flink-savepoints-directory
{% endhighlight %}

**Expected Output**
{% highlight bash %}
total 0
  17 drwxr-xr-x   3 root root   60 17 jul 17:05 .
   2 drwxrwxrwt 135 root root 3420 17 jul 17:09 ..
1002 drwxr-xr-x   2 root root  140 17 jul 17:05 savepoint-<short-job-id>-<uuid>
{% endhighlight %}

#### Step 2a: Restart Job without Changes

You can now restart the upgraded Job from this Savepoint. For simplicity, you can start by 
restarting it without any changes.

<div class="codetabs" markdown="1">
<div data-lang="CLI" markdown="1">
**Command**
{% highlight bash %}
docker-compose run --no-deps client flink run -s <savepoint-path> -d /opt/flink/examples/streaming/ClickEventCount.jar --bootstrap.servers kafka:9092 --checkpointing --event-time
{% endhighlight %}
**Expected Output**
{% highlight bash %}
Starting execution of program
Job has been submitted with JobID <job-id>
{% endhighlight %}
</div>
<div data-lang="REST API" markdown="1">

**Request**
{% highlight bash %}
# Uploading the JAR
docker-compose run --no-deps client curl -X POST -H "Expect:" -F "jarfile=@/opt/flink/examples/streaming/ClickEventCount.jar" http://jobmanager:8081/jars/upload
{% endhighlight %}

**Expected Response (pretty-printed)**
{% highlight json %}
{
  "filename": "/tmp/flink-web-<uuid>/flink-web-upload/<jar-id>",
  "status": "success"
}

{% endhighlight %}

**Request**
{% highlight bash %}
# Submitting the Job
docker-compose run --no-deps client curl -X POST http://jobmanager:8081/jars/<jar-id>/run -d {"programArgs": "--bootstrap.servers kafka:9092 --checkpointing --event-time", "savepointPath": "<savepoint-path>"}
{% endhighlight %}
**Expected Response (pretty-printed)**
{% highlight json %}
{
  "jobid": "<job-id>"
}
{% endhighlight %}
</div>
</div>

Once the Job is "RUNNING" again, you will see in the *output* Topic that now data was lost during 
the upgrade: all windows are present with a count of exactly one thousand.

#### Step 2b: Restart Job with a Different Parallelism (Rescaling)

Alternatively, you could also rescale the Job from this Savepoint by passing a different parallelism
during resubmission.

<div class="codetabs" markdown="1">
<div data-lang="CLI" markdown="1">
**Command**
{% highlight bash %}
docker-compose run --no-deps client flink run -p 3 -s <savepoint-path> -d /opt/flink/examples/streaming/ClickEventCount.jar --bootstrap.servers kafka:9092 --checkpointing --event-time
{% endhighlight %}
**Expected Output**
{% highlight bash %}
Starting execution of program
Job has been submitted with JobID <job-id>
{% endhighlight %}
</div>
<div data-lang="REST API" markdown="1">

**Request**
{% highlight bash %}
# Uploading the JAR
docker-compose run --no-deps client curl -X POST -H "Expect:" -F "jarfile=@/opt/flink/examples/streaming/ClickEventCount.jar" http://jobmanager:8081/jars/upload
{% endhighlight %}

**Expected Response (pretty-printed)**
{% highlight json %}
{
  "filename": "/tmp/flink-web-<uuid>/flink-web-upload/<jar-id>",
  "status": "success"
}

{% endhighlight %}

**Request**
{% highlight bash %}
# Submitting the Job
docker-compose run --no-deps client curl -X POST http://jobmanager:8081/jars/<jar-id>/run -d {"parallelism": 3, "programArgs": "--bootstrap.servers kafka:9092 --checkpointing --event-time", "savepointPath": "<savepoint-path>"}
{% endhighlight %}
**Expected Response (pretty-printed**
{% highlight json %}
{
  "jobid": "<job-id>"
}
{% endhighlight %}
</div>
</div>
Now, the Job has been resubmitted, but it will not start as there are not enough TaskSlots to
execute it with the increased parallelism (1 available, 3 needed). With
{% highlight bash %}
docker-compose scale taskmanager=2
{% endhighlight %}
you can add a second TaskManager to the Flink Cluster, which will automatically register with the 
Flink Master. Shortly after adding the TaskManager the Job should start running again.

Once the Job is "RUNNING" again, you will see in the *output* Topic that now data was lost during 
rescaling: all windows are present with a count of exactly one thousand.

### Querying the Metrics of a Job

The Flink Master exposes all (system & user) [metrics]({{ site.baseurl }}/monitoring/metrics.html)
via its REST API. The endpoint depends on the scope of these metrics.

#### Job Scope

Metrics scoped to a Job can be listed via `jobs/<job-id>/metrics`. The actual value of a metric can
be queried via the `get` query parameter.

**Request**
{% highlight bash %}
docker-compose run --no-deps client curl "jobmanager:8081/jobs/<jod-id>/metrics?get=lastCheckpointSize"
{% endhighlight %}
**Expected Response (pretty-printed; no placeholders)**
{% highlight json %}
[
  {
    "id": "lastCheckpointSize",
    "value": "9378"
  }
]
{% endhighlight %}

#### Task Scope

Metrics scoped to a Task can be listed and queried via `jobs/<job-id>/vertices/<vertex-id>/metrics`.
You first need to inspect the Job resource to find the VertexID of the Vertex(=Task) of interest. 

**Request**
{% highlight bash %}
# find the vertex-id of the vertex of interest
docker-compose run --no-deps client curl jobmanager:8081/jobs/<jod-id>
{% endhighlight %}

**Expected Response (pretty-printed)**
{% highlight json %}
{
  "jid": "<job-id>",
  "name": "Click Event Count",
  "isStoppable": false,
  "state": "RUNNING",
  "start-time": 1563770299330,
  "end-time": -1,
  "duration": 178702,
  "now": 1563770478032,
  "timestamps": {
    "RECONCILING": 0,
    "CREATED": 1563770299330,
    "SUSPENDED": 0,
    "CANCELLING": 0,
    "RUNNING": 1563770299615,
    "FAILED": 0,
    "CANCELED": 0,
    "FAILING": 0,
    "FINISHED": 0,
    "RESTARTING": 0
  },
  "vertices": [
    {
      "id": "<vertex-id>",
      "name": "Source: Custom Source -> Timestamps/Watermarks",
      "parallelism": 2,
      "status": "RUNNING",
      "start-time": 1563770300861,
      "end-time": -1,
      "duration": 177171,
      "tasks": {
        "CANCELED": 0,
        "CREATED": 0,
        "RECONCILING": 0,
        "RUNNING": 2,
        "FAILED": 0,
        "DEPLOYING": 0,
        "SCHEDULED": 0,
        "FINISHED": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 0,
        "read-bytes-complete": true,
        "write-bytes": 528928,
        "write-bytes-complete": true,
        "read-records": 0,
        "read-records-complete": true,
        "write-records": 16793,
        "write-records-complete": true
      }
    },
    {
      "id": "90bea66de1c231edf33913ecd54406c1",
      "name": "Window(TumblingEventTimeWindows(60000), EventTimeTrigger, CountingAggregator, ClickEventStatisticsCollector) -> Sink: Unnamed",
      "parallelism": 2,
      "status": "RUNNING",
      "start-time": 1563770300868,
      "end-time": -1,
      "duration": 177164,
      "tasks": {
        "CANCELED": 0,
        "CREATED": 0,
        "RECONCILING": 0,
        "RUNNING": 2,
        "FAILED": 0,
        "DEPLOYING": 0,
        "SCHEDULED": 0,
        "FINISHED": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 550539,
        "read-bytes-complete": true,
        "write-bytes": 0,
        "write-bytes-complete": true,
        "read-records": 16789,
        "read-records-complete": true,
        "write-records": 0,
        "write-records-complete": true
      }
    }
  ],
  "status-counts": {
    "CANCELED": 0,
    "CREATED": 0,
    "RECONCILING": 0,
    "RUNNING": 2,
    "FAILED": 0,
    "DEPLOYING": 0,
    "SCHEDULED": 0,
    "FINISHED": 0,
    "CANCELING": 0
  },
  "plan": {
    "jid": "<job-id>",
    "name": "Click Event Count",
    "nodes": [
      {
        "id": "<vertex-id>",
        "parallelism": 2,
        "operator": "",
        "operator_strategy": "",
        "description": "Window(TumblingEventTimeWindows(60000), EventTimeTrigger, CountingAggregator, ClickEventStatisticsCollector) -&gt; Sink: Unnamed",
        "inputs": [
          {
            "num": 0,
            "id": "cbc357ccb763df2852fee8c4fc7d55f2",
            "ship_strategy": "HASH",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "<vertex-id>",
        "parallelism": 2,
        "operator": "",
        "operator_strategy": "",
        "description": "Source: Custom Source -&gt; Timestamps/Watermarks",
        "optimizer_properties": {}
      }
    ]
  }
}
{% endhighlight %}

**Request**
{% highlight bash %}
# query numBytesOutPerSecond of SubTask 0 of a vertex
docker-compose run --no-deps client curl "jobmanager:8081/jobs/<job-id>/vertices/<vertex-id>/metrics?get=0.numBytesOutPerSecond
{% endhighlight %}

**Expected Response (pretty-printed)**
{% highlight json %}
[
  {
    "id": "0.numBytesOutPerSecond",
    "value": "3125.05"
  }
]
{% endhighlight %}
{%  top %}

## Variants

You might have noticed that the *Click Event Count* was always started with `--checkpointing` and 
`--event-time` program arguments. By omitting these in the command of the *client* container in the 
`docker-compose.yaml`, you can change the behavior of the Job.

* `--checkpointing` enables [checkpoint]({{ site.baseurl }}/internals/stream_checkpointing.html), 
which is Flink's fault-tolerance mechanism. If you run without it and go through 
[failure and recovery](#observing-failure--recovery), you should will see that data is actually 
lost.

* `--event-time` enables [event time semantics]({{ site.baseurl }}/dev/event_time.html) for your 
Job. When disabled, the Job will assign events to windows based on the wall-clock time instead of 
the timestamp of the `ClickEvent`. Consequently, the number of events per window will not be exactly
one thousand anymore. 
