# parallel-task-runner [![Build Status](https://travis-ci.org/eHarmony/parallel-task-runner.svg?branch=master)](https://travis-ci.org/eHarmony/parallel-task-runner)
A java based task runner with configurable parallelization. Meant for simple tasks that can run in parallel and aggregate data and statistics.

## Usage

```
mvn exec:java

mvn exec:java -Dexec.args="-csv -p false"

mvn exec:java -Dexec.args="--help"
```

To run the Task, use the main class, RunnerApp. It requires the following arguments:

* runner.task.class - The full class name of the task to be run. Eg. com.eharmony.runner.task.sample.WordCountTask
* runner.parser.class - The full class to be used to parse each line in the input file. Eg. com.eharmony.matching.runner.input.IntegerLineParser
* runner.input.file - The full path to the input file used by the task.
* runner.task.threads - The number of threads to run the task
* runner.task.batch.size - The number of entries to be passed into each task

To set these properties, you can use one of two things:

### runner.properties

In the ```config``` folder, there is a template for runner.properties. You must copy runner.properties.template to runner.properties and modify the properties there. Do not commit your own runner.properties.

### Java system properties

You can provide command line arguments that will override the ones provided in runner.properties. For example, ```-Drunner.task.class=SomeClass```, will override the task class used.

### Prompting User

The runner will prompt the user on stdin/stdout to verify the run should continue. Eg.
```
====== Task Properties ======
runner.task.threads: 5
runner.input.file: sample/sample-words.txt
runner.task.class: com.eharmony.runner.task.sample.WordCountTask
runner.task.batch.size: 5
runner.parser.class: com.eharmony.runner.input.StringLineParser

Continue? (Y/n)
```

### Logs

The logs will go to the ```logs``` folder. There is the standard log, matching-task-runner.log, or you can use the output file logger, to log separate data. 
```java
    private static final Logger LOG_TO_FILE = LoggerFactory.getLogger("OUTPUT_FILE_LOGGER");
```


## Overview

### Task

The task runner revolves around the Task interface. You must define your own Task implementation using the task interface.

For example:
```java
public class SomeServiceTask implements Task<Integer, SomeServiceRunnerContext> {
    private static final Logger LOG_TO_FILE = LoggerFactory.getLogger("OUTPUT_FILE_LOGGER");

    @Override
    public boolean executeTask(final Collection<Integer> input, final SomeServiceRunnerContext runnerContext) {
        for (Integer userId : input) {
            User user = runnerContext.getRestClient().get(userId);
            Counters.incrementCounter("USER_COUNT");
...
```

### Runner Context
Each task has its own RunnerContext. A RunnerContext provides the task with the dependencies that task needs. For example:

```java
public class SomeServiceRunnerContext implements RunnerContext {
    private final static String SERVICE_URL = "some.service.url";
    private final static Set<String> REQUIRED_PROPERTIES = Sets.newHashSet(SERVICE_URL);
    private RestClient restClient;

    @Override
    public void init(final Properties properties) throws Exception {
        final String serviceUrl = properties.getProperty(SERVICE_URL);
        restClient = new RestClientImpl(100, 30000, 30000, false);
    }

    @Override
    public Set<String> getRequiredPropertyNames() {
        return REQUIRED_PROPERTIES;
    }

    public RestClient getRestClient() {
        return restClient;
    }
}
```

### Line Parser

The input line parser class determines how to parse each line of input from the input file. For most use cases, you should use the IntegerLineParser, which will attempt to parse each line as an integer. If you have custom input, you can write your own LineParser

### Counters

Most of the tasks require you to count some value. The Counters static utility provides an easy api for incrementing counters, and the RunnerApp will print all counter values at the end of execution.
```java
	if (value == null) {
	    Counters.incrementCounter("NO_VALUE");
	} else {
	    int count = value.getCount();
	    Counters.incrementCounterByValue("SUM_VALUES", count);
	    if (count == 0) {
	        LOG_TO_FILE.info(userId + "\n");
	        Counters.incrementCounter("NO_VALUE");
```

The counter results will be printed in matching-task-runner.log:
```
2017-01-03 12:19:46,136 {main} INFO  [TaskRunner] Finished batches for task ServiceValueCountTask
2017-01-04 10:15:48,112 {main} INFO  [TaskRunner] TOTAL TIME:                       736ms
2017-01-03 12:19:46,136 {main} INFO  [TaskRunner] ========================================================
2017-01-03 12:19:46,136 {main} INFO  [TaskRunner] Counters
2017-01-03 12:19:46,136 {main} INFO  [TaskRunner] ========================================================
2017-01-03 12:19:46,138 {main} INFO  [TaskRunner] NO_VALUE:                 	    8
2017-01-03 12:19:46,138 {main} INFO  [TaskRunner] SUM_VALUES:                 	    3649
2017-01-03 12:19:46,138 {main} INFO  [TaskRunner] TOTAL_USERS:                   	55
2017-01-03 12:19:46,138 {main} INFO  [TaskRunner] ========================================================
```

### Aggregates

Aggregates is a static utility class to perform standard statistics aggregation functions:
* mean
* median
* mode
* min
* max

Example usage:
```java
    Aggregators.aggregate("VALUE_AGGREGATE", value);
```

Example output:
```
2017-01-04 10:15:48,113 {main} INFO  [TaskRunner] ========================================================
2017-01-04 10:15:48,113 {main} INFO  [TaskRunner] Aggregates
2017-01-04 10:15:48,113 {main} INFO  [TaskRunner] ========================================================
2017-01-04 10:15:48,113 {main} INFO  [TaskRunner] --VALUE_AGGREGATE
2017-01-04 10:15:48,114 {main} INFO  [TaskRunner] MEAN:                             154.52
2017-01-04 10:15:48,116 {main} INFO  [TaskRunner] MEDIAN:                           54
2017-01-04 10:15:48,117 {main} INFO  [TaskRunner] MODE:                             0
2017-01-04 10:15:48,117 {main} INFO  [TaskRunner] MIN:                              0
2017-01-04 10:15:48,117 {main} INFO  [TaskRunner] MAX:                              400
2017-01-04 10:15:48,117 {main} INFO  [TaskRunner] ========================================================
```

### Task File Writer

If the logging does not give you enough granualarity, you can use the TaskFileWriter utility. This will open a file for the given file path, under the ```output-files``` folder. The file writer is thread safe and the app takes care of closing it.

```java
	TaskFileWriter fileWriter = TaskFileWriter.getFileWriter("users-no-values.txt");

	fileWriter.write(userId + "\n");
```