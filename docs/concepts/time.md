---
title: Time & The Latency-Completeness Trade-Off
nav-pos: 2
nav-title: Time
nav-parent_id: concepts
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

Time is a unique attribute in data processing because unlike other characteristics, it always moves forward.
However, that does not mean event processing happens in order by perfectly increasing timestamps.
Systems go down, network connections lag, and data consumption often occurs out of order.
How a system handles this out-of-orderness always comes down to a trade-off between latency and completeness. 

* This will be replaced by the TOC
{:toc}

## Latency vs. Completeness in Batch & Stream Processing

In a batch processing system, the batch interval of the job makes the decision about latency vs. completeness automatically.
Imagine a batch job that generates an hourly billing report.
If the job to calculate the 1 pm report begins at 2 pm, immediately at the end of the hour, the results would have low latency but not be complete.
Some number of records from the end of the hour will likely have not yet arrived.
Conversely, if that same job does not run until 6 pm, it will have high latency but be complete.
Most, if not all, records will have arrived within the 5 hours the job waited to start.

In stream processing, this trade-off still exists, but developers have more fine-grained control over the latency vs. completeness in their applications.
In Flink, jobs can handle both known latencies and allowed lateness of records within a system.
Let's use that same billing report as an example, except this time is it written as a continuous streaming application with the following requirements.
Under ideal conditions, records can take up to 10 seconds to reach the system from the time they are generated, and it must include events that arrive up to 5 hours after the end of an hour.
These facts can be encoded into a Flink job to ensure correct results.
Latency can be configured separately on a spectrum. From very low latency, where incremental and updated results are output after every record.
To high latency, where one final result is only output when the processes is sure it is complete. 

## Time Domains

To manage this trade-off of completeness vs. latency in streaming application, Flink offers three different time domains for developers. 

<img src="{{ site.baseurl }}/fig/event_ingestion_processing_time.svg" alt="Event Time, Ingestion Time, and Processing Time" class="offset" width="80%" />

### Processing Time

Processing time refers to the system time of the machine that is executing the respective operation.

When a streaming program runs on processing time, all time-based operations, like time windows, will use the system clock of the machines that run the respective operator.
An hourly processing time window will include all records that arrived at a specific operator between the times when the system clock indicated the full hour.
For example, if an application begins running at 9:15 am, the first hourly processing time window will include eventsprocessed between 9:15 am and 10:00 am, the next window will consist of events processed between 10:00 am and 11:00 am, and so on.

Processing time is the most straightforward notion of time and requires no coordination between streams and machines.
It provides the best performance and the lowest latency. However, in distributed and asynchronous environments processing time does not offer deterministic results.
It is susceptible to the speed records arrive at and flow through the system, along with outages, scheduled or otherwise.

### Event Time

Event time is the time that each event occurred on its producing device.
This time is typically embedded within the records before they enter Flink, and can be extracted from each record.
In event time, the progress of time depends on the data, not on any wall clocks.
Event time programs must specify how to generate *Event Time Watermarks*, which is the mechanism that signals progress in event time. 

In a perfect world, event time processing would yield entirely consistent and deterministic results, regardless of when events arrive, or their ordering.
However, unless the events are known to arrive in-order by timestamp, event time processing incurs some latency while waiting for out-of-order events.
As it is only possible to wait for a finite period, this places a limit on how deterministic event time applications can be.

Assuming all of the data has arrived, event time operations will  produce correct and consistent results; even when working with out-of-order data, late events, or reprocessing historical data.
For example, an hourly event time window will contain all records that carry event timestamps that fall into that hour, regardless of the order they arrive, or when they are processed.

{% info %} Sometimes when event time programs are processing live data in real-time, they will use some *processing time* operations in order to guarantee that they are progressing in a timely fashion.

### Ingestion Time 

Ingestion time is the time that events enter Flink.
At the source operator, each record gets the source's current time as a timestamp, and time-based operations, like time windows, refer to that timestamp.

*Ingestion time* sits conceptually in between *event time* and *processing time*.
Compared to *processing time*, it is slightly more expensive but gives more predictable results.
Because *ingestion time* uses stable timestamps, assigned once at the source, different window operations over the records will refer to the same timestamp.
Whereas in *processing time*, each window operator may assign the record to a different window based on the local system clock and any transport delay.

Compared to *event time*, *ingestion time* programs cannot handle any out-of-order events or late data. However, the programs don't have to specify how to generate *watermarks*.

Internally, *ingestion time* is treated much like *event time*, but with automatic timestamp assignment and automatic watermark generation.

## Making Progress

A stream processor that supports event time needs a way to measure the progress of event time.
A window that computes hourly aggregations needs to know when no more records are expected for a particular hour, so it can close the window and output its results.

Event time can progress independently of processing time, measured by wall clocks.
In one program, the current event time of an operator may trail slightly behind the processing time, accounting for a delay in receiving the events, while both proceed at the same speed.
On the other hand, another streaming program might progress through weeks of event time with only a few seconds of processing, by fast-forwarding through some historical data already buffered in a Kafka topic or another message queue.

The mechanism in Flink to measure progress in event time is watermarks.
Watermarks flow as part of the data stream and carry a timestamp t.
A *Watermark(t)* declares that event time has reached time t, meaning that no more elements are expected to arrive with a timestamp *t' <= t*.
The figure below shows a stream of events with logical timestamps and watermarks flowing inline.
In this example, the events are in order by their timestamps, meaning the 
watermarks are periodic markers in the stream.

<img src="{{ site.baseurl }}/fig/stream_watermark_in_order.svg" alt="A data stream with events (in order) and watermarks" class="center" width="65%" />

Watermarks are crucial for *out-of-order* streams, as illustrated below, where the events are not correctly ordered by time.
A watermark is a declaration that all events up to a specific timestamp should have arrived.
Once a watermark reaches an operator, the operator can advance its internal *event time clock* to the value of the watermark.

<img src="{{ site.baseurl }}/fig/stream_watermark_out_of_order.svg" alt="A data stream with events (out of order) and watermarks" class="center" width="65%" />

Note that event time is inherited by a freshly created stream element, or elements, from either the event that produced them or from watermark that triggered the creation of those elements.

### Propagation of Watermarks

Watermarks are generated at, or directly after, source functions.
Each parallel instance of a source function usually generates its watermarks independently.
These watermarks define the event time at that particular parallel source.

As the watermarks flow through the streaming program, they advance the event time at the operators where they arrive.
Whenever an operator advances its event time, it generates a new watermark downstream for its successor operators.

Some operators consume multiple input streams; a union, for example, or operators following a *keyBy(...)* or *partition(...)* function.
For such an operator, current event time is the minimum of its input streams' event times.
As its input streams update their event times, so does the operator.

The figure below shows an example of events and watermarks flowing through parallel streams, and operators tracking event time.

<img src="{{ site.baseurl }}/fig/parallel_streams_watermarks.svg" alt="Parallel data streams and operators with events and watermarks" class="center" width="80%" />

## Late Elements

Certain elements may violate the watermark condition, meaning that even after the *Watermark(t)* has occurred,
more elements with timestamp *t' <= t* will arrive.
In fact, in many real-world setups, individual elements can be arbitrarily
delayed, making it impossible to specify a time by which all events less than a specific event timestamp have been processed.
Even if the lateness can be bounded, delaying the watermarks by too much is often undesirable.
It will cause too much delay in the evaluation of event time windows.

For this reason, streaming programs may explicitly expect some *late* elements.
Late elements are elements that arrive after the system's event time clock, as signaled by watermarks, has already passed the time of the new element's timestamp.
See [Allowed lateness]({{ site.baseurl }}/dev/stream/operators/windows.html#allowed-lateness) for more information on how to work with late elements in event time windows.

## Futher Resources

Flink implements many techniques from the Dataflow Model.
For more information about event time and watermarks, have a look at the articles below.

  - [Streaming 101](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101) by Tyler Akidau
  - The [Dataflow Model paper](https://research.google.com/pubs/archive/43864.pdf)
