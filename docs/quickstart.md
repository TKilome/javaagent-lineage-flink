# Quickstart

This guide covers practical deployment and local debugging for `javaagent-lineage-flink`.

## Build

Build the distribution package:

```bash
mvn -pl lineage-dist -am package
```

The generated files are:

```text
lineage-dist/target/lineage-dist-1.0.0-SNAPSHOT.tar.gz
lineage-dist/target/lineage-dist-1.0.0-SNAPSHOT.zip
```

## Deploy to Flink

Copy `lineage-core`, one matching Flink version directory, and the reporters you need into `$FLINK_HOME/lib`.
Do not copy multiple Flink version directories into the same Flink JVM.

```bash
tar -zxf lineage-dist-1.0.0-SNAPSHOT.tar.gz -C /opt
cp /opt/javaagent-lineage-flink-1.0.0-SNAPSHOT/lib/common/*.jar $FLINK_HOME/lib/
cp /opt/javaagent-lineage-flink-1.0.0-SNAPSHOT/lib/flink-1.19/*.jar $FLINK_HOME/lib/
cp /opt/javaagent-lineage-flink-1.0.0-SNAPSHOT/lib/reporter/*.jar $FLINK_HOME/lib/
```

Then add only the core jar as the Java Agent:

```bash
-javaagent:./lib/lineage-core-1.0.0-SNAPSHOT.jar
```

Do not pass parser, instrumentation, or reporter jars to `-javaagent`. They are discovered from the Flink classpath through `ServiceLoader`.

## Session Cluster

For a Flink session cluster, the Java Agent must be installed in the JVM that generates the `JobGraph`.

There are two common submission paths:

- Local `flink run` client: the local client usually executes the user program and generates the `JobGraph`, then submits it to the session cluster.
- Web UI jar submission: the request goes through the JobManager, so the JobManager generates or loads the `JobGraph` on the cluster side.

### Local flink run

Install the lineage jars into the local Flink client classpath:

```bash
cp /opt/javaagent-lineage-flink-1.0.0-SNAPSHOT/lib/common/*.jar $FLINK_HOME/lib/
cp /opt/javaagent-lineage-flink-1.0.0-SNAPSHOT/lib/flink-1.19/*.jar $FLINK_HOME/lib/
cp /opt/javaagent-lineage-flink-1.0.0-SNAPSHOT/lib/reporter/*.jar $FLINK_HOME/lib/
```

For Flink 1.20, replace `lib/flink-1.19` with `lib/flink-1.20`.

Run the local client with the Java Agent:

```bash
JVM_ARGS="-javaagent:$FLINK_HOME/lib/lineage-core-1.0.0-SNAPSHOT.jar" \
bin/flink run \
  -m jm-host:8081 \
  /data/jobs/order-sql-job.jar
```

For a persistent client-side setting, configure `env.java.opts.client` in `flink-conf.yaml`. The key point is that the `flink run` client JVM must load `lineage-core`, and the instrumentation/parser/reporter jars must be visible on the client classpath.

### Web UI Submission

For Web UI submission, install the lineage jars on every JobManager node:

```bash
cp /opt/javaagent-lineage-flink-1.0.0-SNAPSHOT/lib/common/*.jar $FLINK_HOME/lib/
cp /opt/javaagent-lineage-flink-1.0.0-SNAPSHOT/lib/flink-1.19/*.jar $FLINK_HOME/lib/
cp /opt/javaagent-lineage-flink-1.0.0-SNAPSHOT/lib/reporter/*.jar $FLINK_HOME/lib/
```

For Flink 1.20, replace `lib/flink-1.19` with `lib/flink-1.20`.

Add the following option to the JobManager JVM options in `flink-conf.yaml`:

```yaml
env.java.opts.jobmanager: "-javaagent:./lib/lineage-core-1.0.0-SNAPSHOT.jar"
```

Then restart the session cluster.

1. Open the Flink Web UI.
2. Upload the job jar.
3. Submit the job from the Web UI.

This path goes through the JobManager. As long as the JobManager JVM has the `-javaagent` option and the lineage jars are in `$FLINK_HOME/lib`, the agent can intercept `PipelineExecutorUtils#getJobGraph(...)` and parse lineage.

### Flink SQL

For Flink SQL jobs, the same rule applies: install the agent in the process that generates the `JobGraph`.

- If SQL is packaged as a jar and submitted by local `flink run`, add the Java Agent to the local client JVM.
- If SQL is submitted through the Web UI, add the Java Agent to the JobManager JVM.

```text
JVM that generates JobGraph
  ├─ -javaagent: lineage-core
  └─ classpath contains instrumentation/parser/reporter jars
```

## YARN Application Mode

Install the distribution jars into the Flink installation used by the YARN application cluster:

```bash
cp /opt/javaagent-lineage-flink-1.0.0-SNAPSHOT/lib/common/*.jar $FLINK_HOME/lib/
cp /opt/javaagent-lineage-flink-1.0.0-SNAPSHOT/lib/flink-1.19/*.jar $FLINK_HOME/lib/
cp /opt/javaagent-lineage-flink-1.0.0-SNAPSHOT/lib/reporter/*.jar $FLINK_HOME/lib/
```

For Flink 1.20, replace `lib/flink-1.19` with `lib/flink-1.20`.

Submit a job:

```bash
bin/flink run-application \
  -t yarn-application \
  -Denv.java.opts.jobmanager="-javaagent:./lib/lineage-core-1.0.0-SNAPSHOT.jar" \
  /data/jobs/order-job.jar
```

Notes:

- The agent runs in the JobManager process.
- The current implementation parses lineage when Flink generates the `JobGraph`.
- The adapter jars must be present in the Flink runtime classpath.
- If the jars do not match the actual Flink or connector version, startup, instrumentation, or parsing may fail.

## HTTP Reporter

`lineage-reporter-http` posts the final `LineageEvent` to an HTTP endpoint synchronously. Put the jar into `$FLINK_HOME/lib` and configure the endpoint on the JVM that generates the `JobGraph`.

The HTTP reporter properties are JVM system properties. Put them inside the same JVM options value as `-javaagent`; do not pass them as top-level Flink command options.

For a session cluster or Web UI submission, configure the JobManager JVM options in `flink-conf.yaml`:

```yaml
env.java.opts.jobmanager: "-javaagent:./lib/lineage-core-1.0.0-SNAPSHOT.jar -Dlineage.reporter.http.url=http://lineage-service:8080/api/flink/lineage -Dlineage.reporter.http.token=xxx -Dlineage.reporter.http.connectTimeoutMs=3000 -Dlineage.reporter.http.readTimeoutMs=5000 -Dlineage.reporter.http.retries=1"
```

For YARN application mode, pass them inside `-Denv.java.opts.jobmanager`:

```bash
bin/flink run-application \
  -t yarn-application \
  -Denv.java.opts.jobmanager="-javaagent:./lib/lineage-core-1.0.0-SNAPSHOT.jar \
    -Dlineage.reporter.http.url=http://lineage-service:8080/api/flink/lineage \
    -Dlineage.reporter.http.token=xxx \
    -Dlineage.reporter.http.connectTimeoutMs=3000 \
    -Dlineage.reporter.http.readTimeoutMs=5000 \
    -Dlineage.reporter.http.retries=1" \
  /data/jobs/order-job.jar
```

`lineage.reporter.http.url` is required. If HTTP reporting fails, job submission fails.

## Local Debug

Build the agent jar first:

```bash
mvn -o -pl lineage-core -am package
```

Run this test in IDEA:

```text
KafkaLineageAgentJobGraphDebugTest
```

VM options:

```text
-ea -javaagent:/absolute/path/to/javaagent-lineage-flink/lineage-core/target/lineage-core-1.0.0-SNAPSHOT.jar
```

The test builds a real `KafkaSource -> map -> KafkaSink` pipeline and calls `PipelineExecutorUtils#getJobGraph(...)`. It does not connect to Kafka and does not submit a Flink job.

Useful breakpoints:

```text
io.github.tkilome.flink.lineage.agent.LineageAgent#premain
org.apache.flink.client.deployment.executors.PipelineExecutorUtils#getJobGraph
io.github.tkilome.flink.lineage.runtime.LineageRuntime#processLineage
io.github.tkilome.flink.lineage.parser.kafka.KafkaLineageParserFactory#parseKafkaEndpoints
io.github.tkilome.flink.lineage.reporter.logging.LoggingLineageReporter#report
```

Byte Buddy advice is inlined by default, so a breakpoint inside the advice method may not be hit directly. Break on the target Flink method or on `LineageRuntime#processLineage` for normal debugging.
