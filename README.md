# SparkHistoryServerExtensions
Spark History Server Extensions

The goal of this project is to add extensions to Spark History Server which will allow it to be customized for specific needs.
In current release these extensions will enable servicing worker logs from local or cloud storage which is not dependent on the spark worker UI to be up.

As part of these extensions, a customized Executors page is injected into the spark ui tree replacing the standard page and behavior.
This is done by putting the shs extensions jar as and entry in jar loading sequence before spark ui jars.
Spark UI Executor Page will render the STDOUT and STDERR log page URLs pointing to history server itself instead of pointing to worker UI.
Behind this URL, there is a enhanced version of LogPage to server the requestes for logs from local or cloud storage based on a newly introduced spark setting spark.logs.home.dir which can be set to local storage accessible SHS or to a cloud bucket (ADLS, GC, S3). if using cloud bucket, make sure to set the core_site.xml as per specific cloud bucket. 

Steps to use the this extension
1. Either build the jar or download the current build 
2. Put the jar file in SPARK_HOME/jar - make sure jat has a numeric number in start to allow loading this jar before standard spark jars
3. set spark.logs.home.dir and  point it to the location where you will put the worker logs. Folder structure of the logs must be same as hoe this is generated on the      worker as <spark_worker_dir>/<app_id>/<executir_id>/<log files>
4. Setup a log forwarding from spark workers to push the worker logs to the location in #3.
5. Set your spark events as you will do normally. no change to it.
6. Start Spark History Server

After this deployment, you should be able to access Spark History Server UI just like you will do without any extensions.
When you click on the logs STDOUT or STDERR, logs will be servers by history server itself instead of redirecting the request to spark worker ui.

Note: Do not use this jar with your spark cluster installation where you need standard master UI as this will inrefere with the standdard executor functionality and you will not be able to see the logs of running applications. These extensions are intended to be used with a standalone installation of Spark History Server.
