/*
 * Copyright 2018 eHarmony, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.eharmony.runner;

import com.eharmony.runner.input.LineParser;
import com.eharmony.runner.output.CsvStatisticsOutputWriter;
import com.eharmony.runner.task.Task;
import com.eharmony.runner.task.TaskRunner;
import com.google.common.collect.Sets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class RunnerApp {
    private static final Logger LOG = LoggerFactory.getLogger(RunnerApp.class);
    private static final String RUNNER_TASK_CLASS = "runner.task.class";
    private static final String RUNNER_PARSER_CLASS = "runner.parser.class";
    private static final String RUNNER_INPUT_FILE = "runner.input.file";
    private static final String RUNNER_TASK_THREADS = "runner.task.threads";
    private static final String RUNNER_TASK_BATCH_SIZE = "runner.task.batch.size";
    private static final String RUNNER_INPUT_SKIP_SIZE = "runner.input.skip.size";
    private static final String RUNNER_INPUT_PROCESS_SIZE = "runner.input.process.size";
    private static final String DEFAULT_CONFIG_PATH = "config/runner.properties";
    private static final Set<String> REQUIRED_PROPERTIES =
            Sets.newHashSet(RUNNER_INPUT_FILE,
                    RUNNER_PARSER_CLASS,
                    RUNNER_TASK_BATCH_SIZE,
                    RUNNER_TASK_CLASS,
                    RUNNER_TASK_THREADS);

    private static Options OPTIONS = new Options();
    static {
        OPTIONS.addOption("c", "config", true, "Custom location for a configuration properties file. " +
                "Default is ./config/runner.properties");
        OPTIONS.addOption("p", "prompt", true, "Prompt to confirm task settings before executing task. " +
                "Default is true");
        OPTIONS.addOption("csv", false, "Outputs the counters and aggregators as csv files. Default is to only log.");
        OPTIONS.addOption("h", "help", false, "Print this message.");
    }

    public static void main(final String[] args) {
        Properties runnerProperties = new Properties();

        try {
            RunnerOptions options = parseOptions(args);
            runnerProperties.load(new FileInputStream(options.configPath));
            runnerProperties.putAll(System.getProperties());

            validateProperties(REQUIRED_PROPERTIES, runnerProperties);

            Class runnerTask = Class.forName(runnerProperties.getProperty(RUNNER_TASK_CLASS));
            Task task = (Task) runnerTask.newInstance();

            Class runnerContext = task.getRunnerContextClass();
            RunnerContext context = (RunnerContext) runnerContext.newInstance();

            validateProperties(context.getRequiredPropertyNames(), runnerProperties);

            context.init(runnerProperties);

            Class runnerParser = Class.forName(runnerProperties.getProperty(RUNNER_PARSER_CLASS));
            LineParser parser = (LineParser) runnerParser.newInstance();

            File inputFile = new File(runnerProperties.getProperty(RUNNER_INPUT_FILE));

            TaskRunner runner;
            final int numThreads = Integer.parseInt(runnerProperties.getProperty(RUNNER_TASK_THREADS));
            final int batchSize = Integer.parseInt(runnerProperties.getProperty(RUNNER_TASK_BATCH_SIZE));

            int skipSize = Integer.parseInt(runnerProperties.getProperty(RUNNER_INPUT_SKIP_SIZE,"0"));
            int inputProcessSize = Integer.parseInt(
                    runnerProperties.getProperty(RUNNER_INPUT_PROCESS_SIZE,Integer.toString(Integer.MAX_VALUE)));

            if (options.outputCsv) {
                runner = new TaskRunner(numThreads, batchSize, Optional.of(new CsvStatisticsOutputWriter()),
                        skipSize,inputProcessSize);
            } else {
                runner = new TaskRunner(numThreads, batchSize,Optional.empty(),skipSize,inputProcessSize);
            }

            Set<String> allProperties = new HashSet<>(REQUIRED_PROPERTIES);
            allProperties.addAll(context.getRequiredPropertyNames());
            if (!options.promptUser || promptUser(runnerProperties, allProperties)) {
                System.out.println("Starting Process with given parameters...");
                final long startTime = System.nanoTime();
                runner.executeTask(inputFile, parser, task, context);
                final long endTime = System.nanoTime();
                printExecutionTime(startTime, endTime);
            } else {
                LOG.info("User exited");
                System.exit(-6);
            }

        } catch (ParseException e) {
            LOG.error("Unable to parse command line arguments: ", e);
            System.exit(-6);
        } catch (IllegalArgumentException e) {
            LOG.error("", e);
            System.exit(-5);
        } catch (ClassNotFoundException e) {
            LOG.error("Unable to load class", e);
            System.exit(-4);
        } catch (InstantiationException | IllegalAccessException e) {
            LOG.error("Unable to instantiate  class", e);
            System.exit(-3);
        } catch (IOException e) {
            LOG.error("Unable to load properties file", e);
            System.exit(-2);
        } catch (Exception e) {
            LOG.error("An error occurred", e);
            System.exit(-1);
        }
        System.exit(0);
    }

    private static RunnerOptions parseOptions(final String[] args) throws ParseException {
        CommandLineParser commandLineParser = new DefaultParser();
        CommandLine line = commandLineParser.parse(OPTIONS, args);

        if (!line.hasOption("h")) {
            boolean promptUser = Boolean.parseBoolean(line.getOptionValue("p", "true"));
            String configPath = line.getOptionValue("c", DEFAULT_CONFIG_PATH);
            return new RunnerOptions(promptUser, configPath, line.hasOption("csv"));
        } else {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "mvn exec:java", OPTIONS);
            System.exit(0);
            return null;
        }
    }

    private static class RunnerOptions {
        boolean promptUser;
        String configPath;
        boolean outputCsv;

        RunnerOptions(final boolean promptUser,
                      final String configPath,
                      final boolean outputCsv) {
            this.promptUser = promptUser;
            this.configPath = configPath;
            this.outputCsv = outputCsv;
        }
    }

    private static boolean promptUser(final Properties runnerProperties,
                                      final Set<String> inputProperties) {
        System.out.println("====== Task Properties ======");
        for (String propertyName : inputProperties) {
            System.out.println(propertyName + ": " + runnerProperties.get(propertyName));
        }
        System.out.println("\nContinue? (Y/n)");
        Scanner input = new Scanner(System.in);
        String confirmation = input.nextLine();
        return confirmation.toLowerCase().equals("y") || confirmation.toLowerCase().equals("yes");
    }

    private static void validateProperties(final Set<String> requiredProperties,
                                           final Properties runnerProperties) {
        for (String requiredProperty : requiredProperties) {
            if (!runnerProperties.containsKey(requiredProperty)) {
                throw new IllegalArgumentException("Missing required property: " + requiredProperty);
            }
        }
    }

    private static void printExecutionTime(final long startTime, final long endTime) {
        final long duration = endTime - startTime;
        final long durationInSeconds = TimeUnit.NANOSECONDS.toSeconds(duration);
        LOG.info("Task took {} minutes and {} seconds to finish.", durationInSeconds/60, durationInSeconds%60);
    }
}