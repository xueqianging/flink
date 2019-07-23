---
title: "Local Setup"
nav-title: 'Local Setup'
nav-parent_id: deployment
nav-pos: 0
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

Get a local Flink cluster up and running to runs tests and example Jobs.
Flink runs on __Linux, Mac OS X, and Windows__. To be able to run Flink, the only requirement is to have a working __Java 8.x__ installation.

You can check the correct installation of Java by issuing the following command:

{% highlight bash %}
java -version
{% endhighlight %}

If you have Java 8, the output will look something like this:

{% highlight bash %}
java version "1.8.0_111"
Java(TM) SE Runtime Environment (build 1.8.0_111-b14)
Java HotSpot(TM) 64-Bit Server VM (build 25.111-b14, mixed mode)
{% endhighlight %}

* This will be replaced by the TOC
{:toc}

{% if site.is_stable %}
<div class="codetabs" markdown="1">

<div data-lang="Download and Unpack" markdown="1">
1. Download a binary from the [downloads page](http://flink.apache.org/downloads.html). You can pick
   any Hadoop/Scala combination you like. If you plan to just use the local file system, any Hadoop
   version will work fine.
2. Go to the download directory.
3. Unpack the downloaded archive.

{% highlight bash %}
$ cd ~/Downloads        # Go to download directory
$ tar xzf flink-*.tgz   # Unpack the downloaded archive
$ cd flink-{{site.version}}
{% endhighlight %}
</div>

<div data-lang="MacOS X" markdown="1">
For MacOS X users, Flink can be installed through [Homebrew](https://brew.sh/).

{% highlight bash %}
$ brew install apache-flink
...
$ flink --version
Version: 1.2.0, Commit ID: 1c659cf
{% endhighlight %}
</div>

</div>

{% else %}
### Download and Compile
Clone the source code from one of our [repositories](http://flink.apache.org/community.html#source-code), e.g.:

{% highlight bash %}
$ git clone https://github.com/apache/flink.git
$ cd flink
$ mvn clean package -DskipTests # this will take up to 10 minutes
$ cd build-target               # this is where Flink is installed to
{% endhighlight %}
{% endif %}

### Start a Local Flink Cluster

{% highlight bash %}
$ ./bin/start-cluster.sh  # Start Flink
{% endhighlight %}

Check the __Dispatcher's web frontend__ at [http://localhost:8081](http://localhost:8081) and make sure everything is up and running. The web frontend should report a single available TaskManager instance.

<a href="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-1.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-1.png" alt="Dispatcher: Overview"/></a>

You can also verify that the system is running by checking the log files in the `logs` directory:

{% highlight bash %}
$ tail log/flink-*-standalonesession-*.log
INFO ... - Rest endpoint listening at localhost:8081
INFO ... - http://localhost:8081 was granted leadership ...
INFO ... - Web frontend listening at http://localhost:8081.
INFO ... - Starting RPC endpoint for StandaloneResourceManager at akka://flink/user/resourcemanager .
INFO ... - Starting RPC endpoint for StandaloneDispatcher at akka://flink/user/dispatcher .
INFO ... - ResourceManager akka.tcp://flink@localhost:6123/user/resourcemanager was granted leadership ...
INFO ... - Starting the SlotManager.
INFO ... - Dispatcher akka.tcp://flink@localhost:6123/user/dispatcher was granted leadership ...
INFO ... - Recovering all persisted jobs.
INFO ... - Registering TaskManager ... under ... at the SlotManager.
{% endhighlight %}

{% highlight bash %}
$ ./bin/stop-cluster.sh
{% endhighlight %}

If you want to run Flink locally on a Windows machine you need to [download](http://flink.apache.org/downloads.html) and unpack the binary Flink distribution. After that you can either use the **Windows Batch** file (`.bat`), or use **Cygwin** to run the Flink Jobmanager.

## Starting with Windows Batch Files

To start Flink in from the *Windows Command Line*, open the command window, navigate to the `bin/` directory of Flink and run `start-cluster.bat`.

Note: The ``bin`` folder of your Java Runtime Environment must be included in Window's ``%PATH%`` variable. Follow this [guide](http://www.java.com/en/download/help/path.xml) to add Java to the ``%PATH%`` variable.

{% highlight bash %}
$ cd flink
$ cd bin
$ start-cluster.bat
Starting a local cluster with one JobManager process and one TaskManager process.
You can terminate the processes via CTRL-C in the spawned shell windows.
Web interface by default on http://localhost:8081/.
{% endhighlight %}

After that, you need to open a second terminal to run jobs using `flink.bat`.

{% top %}

## Starting with Cygwin and Unix Scripts

With *Cygwin* you need to start the Cygwin Terminal, navigate to your Flink directory and run the `start-cluster.sh` script:

{% highlight bash %}
$ cd flink
$ bin/start-cluster.sh
Starting cluster.
{% endhighlight %}

{% top %}

## Installing Flink from Git

If you are installing Flink from the git repository and you are using the Windows git shell, Cygwin can produce a failure similar to this one:

{% highlight bash %}
c:/flink/bin/start-cluster.sh: line 30: $'\r': command not found
{% endhighlight %}

This error occurs because git is automatically transforming UNIX line endings to Windows style line endings when running in Windows. The problem is that Cygwin can only deal with UNIX style line endings. The solution is to adjust the Cygwin settings to deal with the correct line endings by following these three steps:

1. Start a Cygwin shell.

2. Determine your home directory by entering

    {% highlight bash %}
    cd; pwd
    {% endhighlight %}

    This will return a path under the Cygwin root path.

3. Using NotePad, WordPad or a different text editor open the file `.bash_profile` in the home directory and append the following: (If the file does not exist you will have to create it)

{% highlight bash %}
export SHELLOPTS
set -o igncr
{% endhighlight %}

Save the file and open a new bash shell.

{% top %}
