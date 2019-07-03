---
title: "DataStream API Tutorial"
nav-id: datastreamapitutorials
nav-title: 'DataStream API Tutorial'
nav-parent_id: apitutorials
nav-pos: 2
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

Apache Flink offers a DataStream API for building robust, stateful streaming applications. It provides fine-grained control over time and state, which can allow for the implementation of complex event-driven applications.

* This will be replaced by the TOC
{:toc}

## What Are We Building? 

Credit card fraud is a growing concern in the digital age. One way criminals commit fraud is to get the card information of their victim and make a duplicate card. Then they test it by making one or more small purchases, often for a dollar or less. If it works, they then make more significant purchases to get items they can sell or keep for themselves.

In this tutorial, we'll show how to build a fraud detection system for detecting fraudulent credit card transactions. We will start by start by defining a simple set of rules and see how Flink allows us to implement advanced business logic and act in real time. 

## Prerequisites

We'll assume that you have some familiarity with Java or Scala, but you should be able to follow along even if you're coming from a different programming language.

If you want to follow along, you will require a computer with: 

* Java 8 
* Maven 

## Help, I’m Stuck! 

If you get stuck, check out the [community support resources](https://flink.apache.org/community.html).
In particular, Apache Flink's [user mailing](https://flink.apache.org/community.html#mailing-lists) list is consistently rated as one of the most active of any Apache project and a great way to get help quickly. 


## How To Follow Along

If you would like to follow along this walkthrough provides a Flink Maven Archetype to create a skeleton project with all the necessary dependencies quickly.

{% highlight bash %}
$ mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-java \{% unless site.is_stable %}
    -DarchetypeCatalog=https://repository.apache.org/content/repositories/snapshots/ \{% endunless %}
    -DarchetypeVersion={{ site.version }} \
    -DgroupId=fraud-detection \
    -DartifactId=fraud-detection \
    -Dversion=0.1 \
    -Dpackage=frauddetection \
    -DinteractiveMode=false
{% endhighlight %}

{% unless site.is_stable %}
<p style="border-radius: 5px; padding: 5px" class="bg-danger">
    <b>Note</b>: For Maven 3.0 or higher, it is no longer possible to specify the repository (-DarchetypeCatalog) via the commandline. If you wish to use the snapshot repository, you need to add a repository entry to your settings.xml. For details about this change, please refer to <a href="http://maven.apache.org/archetype/maven-archetype-plugin/archetype-repository.html">Maven official document</a>
</p>
{% endunless %}

You can edit the `groupId`, `artifactId` and `package` if you like. With the above parameters,
Maven will create a project with all the dependencies to complete this tutorial.
After importing the project in your editor, you will see a file following code. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
package frauddetection;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class FraudDetectionJob {

    public static final double SMALL_AMOUNT = 0.01;

    public static final double LARGE_AMOUNT = 500.00;

    public static final long ONE_DAY = 24 * 60 * 60 * 1000;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
            .addSource(new TransactionSource())
            .name("transactions");
        
        DataStream<Alerts> alerts = transactions
            .keyBy(Transaction::getAccountId)
            .process(new FraudDetector())
            .name("fraud-detector");

        alerts
            .addSink(new AlertSink())
            .name("send-alerts");

        env.execute("Fraud Detection");
    }

    public static class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

        @Override
        public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {
  
            collector.collect(new Alert(transaction));
        }
    }
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
package frauddetection

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.scala.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction
import org.apache.flink.walkthrough.common.source.TransactionSource

object FraudDetectionJob {

    val SMALL_AMOUNT = 0.01

    val LARGE_AMOUNT = 500.00

    val ONE_DAY = 24 * 60 * 60 * 1000

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val transactions = env
            .addSource(new TransactionSource)
            .name("transactions")
        
        val alerts = transactions
            .keyBy(transaction => transaction.getAccountId)
            .process(new FraudDetector)
            .name("fraud-detector")

        alerts
            .addSink(new AlertSink)
            .name("send-alerts")

        env.execute("Fraud Detection")
    }

    class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

        override def processElement(
            transaction: Transaction,
            context: Context,
            collector: Collector[Alert]): Unit = {

            collector.collect(new Alert(transaction))
        }
    }
}
{% endhighlight %}
</div>
</div>

Let's break down this code by component. 

## Breaking Down The Code

#### The Execution Environment

The first line sets up our `StreamExecutionEnvironment`.
The execution environment is how we set properties for our deployments, specify whether we are writing a batch or streaming application, and create our sources.
Here we have chosen to use the stream environment since we are building a streaming application.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
{% endhighlight %}
</div>
</div>

#### Creating A Source

Sources define connections to external systems that Flink can use to consume data. We are adding a source that produces an infinite stream of credit card transactions for us to process. 
A transaction contains the account ID's (`accountId`), timestamps (`timestamp`) of when the sale occurred, and US$ amounts (`amount`). 
The `name` attached to our source is just for debugging purposes, so if something goes wrong, we will know where the error originated.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Transaction> transactions = env
    .addSource(new TransactionSource())
    .name("transactions")
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val transactions = env
    .addSource(new TransactionSource)
    .name("transactions")
{% endhighlight %}
</div>
</div>

#### Detecting Fraud

Next, we partition our stream by the account id so that each transaction for a given account will be processed by an operator that will see every purchase for that account.
This partitioned stream is what is processed by our `FraudDetector`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Alerts> alerts = transactions
    .keyBy(Transaction::getAccountId)
    .process(new FraudDetector())
    .name("fraud-detector");
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val alerts = transactions
    .keyBy(transaction => transaction.getAccountId)
    .process(new FraudDetector)
    .name("fraud-detector")
{% endhighlight %}
</div>
</div>

#### Outputting Results
 
Sink's connect Flink jobs to external systems to output events.
The `AlertSink` writes our results to standard output so we can easily see our results.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
alerts.addSink(new AlertSink());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
alerts.addSink(new AlertSink)
{% endhighlight %}
</div>
</div>

#### Executing The Job

Flink applications are built lazily shipped to the cluster for execution only once fully formed. 
We call `StreamExecutionEnvironment#execute` to begin the execution of our job. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
env.execute("Fraud Detection");
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
env.execute("Fraud Detection")
{% endhighlight %}
</div>
</div>


## Writing An Initial Application 

For our initial implementation, we will define fraud as an account that makes a small transaction followed by a significant transaction; where a small purchase is less than $0.10 immediately followed by a large purchase greater than $500.
To do this, we must _remember_ information across events; a large transaction is only fraudulent if the previous amount was small.
Remembering information across events requires `state` and so we will implement our `FraudDetector` using a `KeyedProcessFunction` which provides fine-grained control over state and time.

What we would like is to be able to set a flag when a small transaction is processed; that way, when a large transaction comes through we can check our flag and determine if what we are seeing is fraud.
The flag is what we want to store in state.

The most basic type of state in Flink is defined using `ValueState`, a special data type that provides _fault tolerant_, _managed_, _per key_ state.
They are defined by providing a `ValueStateDescriptor` which contains metadata about how the state should be managed.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public static class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private transient ValueState<Boolean> flagState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>(
            "flag",
            Types.BOOLEAN);

        flagState = getRuntimeContext().getState(descriptor);
    }
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

    @transient var flagState: ValueState[Boolean] = _

    override def open(parameters: Configuration): Unit = {
        val descriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)

        flagState = getRuntimeContext.getState(descriptor)
    }
{% endhighlight %}
</div>
</div>

`ValueState` is a wrapper class, similar to `AtomicReference` in the Java standard library.
It provides three methods for interacting with its contents; `update` sets the current state, `value` gets the current value for the state, and `clear` to delete the current state.
Otherwise, fault tolerance is managed automatically under the hood, and so you can interact with it like any standard variable.

For our fraud detector, when an element arrives, we want to check and see if our flag is set. If it has and this transaction is larger than $500 we will output an alert. If the transaction is less than $0.10, we want to set the flag to true. Moreover, we always need to clear the flag after the first event after a small transaction.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
    @Override
    public void processElement(
        Transaction transaction,
        Context context,
        Collector<Alert> collector) throws Exception {

        // Get the current state for the current key
        Boolean lastTransactionWasSmall = flagState.value();

        if (lastTransactionWasSmall) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                //Output an alert downstream
                collector.collect(new Alert(transaction));
            }
        }
        
        // clean up our state
        flagState.clear();

        if (transaction.getAmount() < SMALL_AMOUNT) {
            // set the flag to true
            flagState.update(true);
        }
    }
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
    override def processElement(
        transaction: Transaction,
        context: Context,
        collector: Collector[Alert]): Unit = {

        // Get the current state for the current key
        val lastTransactionWasSmall = flagState.value

        if (lastTransactionWasSmall) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                //Output an alert downstream
                collector.collect(new Alert(transaction))
            }
        }
        
        // clean up our state
        flagState.clear()

        if (transaction.getAmount() < SMALL_AMOUNT) {
            // set the flag to true
            flagState.update(true)
        }
    }
}
{% endhighlight %}
</div>
</div>
## State + Time &#10084;&#65039; 

Many event-driven applications require a strong notion of time to complement their state.
For example, suppose we wanted to set a 24-hour timeout to our fraud detector.
Flink allows setting timers which serve as callbacks at some point in time in the future.

Along with the previous requirements, we will now add new features to our application.

* Whenever the flag is set to true, also set a timer for 24 hours in the future.
* When the timer fires, reset the flag by clearing its state
* If the flag is ever cleared the timer should be canceled.

To be able to cancel a timer, we have to remember what time it is set for, and remembering implies state, so we'll begin by creating a timer state along with our flag state.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public static class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private transient ValueState<Boolean> flagState;

    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
            "flag",
            Types.BOOLEAN);

        flagState = getRuntimeContext().getState(descriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
            "timer-state",
            Types.Long);

        timerState = getRuntimeContext().getState(descriptor);
    }
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

    @transient var flagState: ValueState[Boolean] = _

    @transient var timerState: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
        val descriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)

        flagState = getRuntimeContext.getState(descriptor)

        timerDescriptor = new ValueStateDescriptor(
            "timer-state",
            Types.Long)

        timerState = getRuntimeContext().getState(descriptor)
    }
{% endhighlight %}
</div>
</div>

`KeyedProcessFunction`'s all called with a `Context` that contains a timer service.
The timer service allows us to query the current time, register timers, and delete timers.
Using this, we can set a timer for 24 hours in the future every time the flag is set and store the timestamp in `timerState`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
        } else if (transaction.getAmount() < SMALL_AMOUNT) {
            state.update(true);

            long timer = context.timerService().currentProcessingTime() + ONE_DAY;
            context.timerService().registerProcessingTimeTimer(timer);

            timerState.update(timer);
        }
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
        } else if (transaction.getAmount() < SMALL_AMOUNT) {
            state.update(true)

            val timer = context.timerService().currentProcessingTime() + ONE_DAY
            context.timerService().registerProcessingTimeTimer(timer)

            timerState.update(timer)
        }
{% endhighlight %}
</div>
</div>

When a timer fires, it calls `KeyedProcessFunction#onTimer`. 
Overriding this method is how we can implement our callback to reset the flag.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        timerState.clear();
        flagState.clear();
    }
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
    override def onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[Alert]): Unit = {
        timerState.clear()
        flagState.clear()
    }
{% endhighlight %}
</div>
</div>

Finally, to cancel the timer, let's add a `cleanUpTimer` method that cancels the current timer and clears the timer state. This method can then be called at the beginning of `processElement`.
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
    private void cleanUpTimer(Context ctx) throws Exception {
        Long timer = timerState.value();

        if (timer != null) { 
            context.timerService().deleteProcessingTimeTimer(timer);
            timerState.clear();
        }
    }
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
    private def cleanUpTimer(ctx: Context): Unit = {
        val timer = timerState.value

        if (timer != null) { 
            context.timerService().deleteProcessingTimeTimer(timer)
            timerState.clear
        }
    }
{% endhighlight %}
</div>
</div>

And that's it, a fully functional, stateful, distributed streaming application!

## Final Application

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
package frauddetection;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class FraudDetectionJob {

    public static final double SMALL_AMOUNT = 0.10;

    public static final double LARGE_AMOUNT = 500.00;

    public static final long ONE_DAY = 24 * 60 * 60 * 1000;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
            .addSource(new TransactionSource())
            .name("transactions");
        
        DataStream<Alerts> alerts = transactions
            .keyBy(Transaction::getAccountId)
            .process(new FraudDetector())
            .name("fraud-detector");

        alerts
            .addSink(new AlertSink())
            .name("send-alerts");

        env.execute("Fraud Detection");
    }

    public static class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

        private transient ValueState<Boolean> flagState;

        private transient ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                    "flag",
                    Types.BOOLEAN);

            flagState = getRuntimeContext().getState(flagDescriptor);

            ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                    "timer-state",
                    Types.LONG);

            timerState = getRuntimeContext().getState(timerDescriptor);
        }

        @Override
        public void processElement(
                Transaction transaction,
                Context context,
                Collector<Alert> collector) throws Exception {
            cleanUpTimer(context);

            Boolean lastTransactionWasSmall = flagState.value();

            if (lastTransactionWasSmall) {
                if (transaction.getAmount() >= LARGE_AMOUNT) {
                    collector.collect(new Alert(transaction));
                }
            }

            flagState.clear();

            if (transaction.getAmount() < SMALL_AMOUNT) {
                flagState.update(true);

                long timer = context.timerService().currentProcessingTime() + ONE_DAY;
                context.timerService().registerProcessingTimeTimer(timer);

                timerState.update(timer);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
            timerState.clear();
            flagState.clear();
        }

        private void cleanUpTimer(Context ctx) throws Exception {
            Long timer = timerState.value();

            if (timer != null) {
                ctx.timerService().deleteProcessingTimeTimer(timerState.value());
                timerState.clear();
            }
        }
    }
}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
package frauddetection

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.scala.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction
import org.apache.flink.walkthrough.common.source.TransactionSource

object FraudDetectionJob {

    val SMALL_AMOUNT = 0.01

    val LARGE_AMOUNT = 500.00

    val ONE_DAY = 24 * 60 * 60 * 1000

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val transactions = env
            .addSource(new TransactionSource)
            .name("transactions")
        
        val alerts = transactions
            .keyBy(transaction => transaction.getAccountId)
            .process(new FraudDetector)
            .name("fraud-detector")

        alerts
            .addSink(new AlertSink)
            .name("send-alerts")

        env.execute("Fraud Detection")
    }

    class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

        @transient var flagState: ValueState[Boolean] = _

        @transient var timerState: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
            val descriptor = new ValueStateDescriptor(
                "flag",
                Types.BOOLEAN)

            flagState = getRuntimeContext.getState(descriptor)

            timerDescriptor = new ValueStateDescriptor(
                "timer-state",
                Types.Long)

            timerState = getRuntimeContext().getState(descriptor)
        }

        override def processElement(
                transaction: Transaction,
                context: Context,
                collector: Collector[Alert]): Unit = {
            cleanUpTimer(context)

            val lastTransactionWasSmall = flagState.value

            if (lastTransactionWasSmall) {
                if (transaction.getAmount() >= LARGE_AMOUNT) {
                    collector.collect(new Alert(transaction))
                }
            }

            flagState.clear

            if (transaction.getAmount() < SMALL_AMOUNT) {
                flagState.update(true)

                long timer = context.timerService().currentProcessingTime() + ONE_DAY
                context.timerService().registerProcessingTimeTimer(timer)

                timerState.update(timer)
            }
        }

        override def onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[Alert]): Unit = {
            timerState.clear()
            flagState.clear()
        }

        private def cleanUpTimer(ctx: Context): Unit = {
            val timer = timerState.value

            if (timer != null) { 
                context.timerService().deleteProcessingTimeTimer(timer)
                timerState.clear
            }
        }
    }

}
{% endhighlight %}
</div>
</div>