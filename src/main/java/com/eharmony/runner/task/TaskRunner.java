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

package com.eharmony.runner.task;

import com.eharmony.runner.RunnerContext;
import com.eharmony.runner.file.TaskFileWriter;
import com.eharmony.runner.input.LineParser;
import com.eharmony.runner.input.LineReader;
import com.eharmony.runner.output.LogStatisticsOutputWriter;
import com.eharmony.runner.output.StatisticsOutputWriter;
import com.eharmony.runner.statistics.Counters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TaskRunner<I, C extends RunnerContext> {
    private static final Logger LOG = LoggerFactory.getLogger(TaskRunner.class);
    private final int batchSize;
    private final int numThreads;
    private final CompletionService<Boolean> completionService;
    private final LogStatisticsOutputWriter logOutputWriter;
    private Optional<StatisticsOutputWriter> alternateOutputWriter;
    private int batchCount;
    private int skipSize;
    private int inputSize;
    private int currInputSize = 0;

    public TaskRunner(final int numThreads, final int batchSize) {
        this(numThreads, batchSize, Optional.empty(),0,Integer.MAX_VALUE);
    }

    public TaskRunner(final int numThreads, final int batchSize,
                      final StatisticsOutputWriter alternateOutputWriter) {
        this(numThreads, batchSize, Optional.of(alternateOutputWriter),0,
                Integer.MAX_VALUE);
    }

    public TaskRunner(final int numThreads, final int batchSize,
                      final Optional<StatisticsOutputWriter> alternateOutputWriter,
                      final int skipRecords,final int inputSize) {
        this.batchSize = batchSize;
        this.numThreads = numThreads;
        this.alternateOutputWriter = alternateOutputWriter;
        this.logOutputWriter = new LogStatisticsOutputWriter();
        this.completionService = new ExecutorCompletionService<>(Executors.newFixedThreadPool(numThreads));
        this.skipSize = skipRecords;
        this.inputSize = inputSize;
    }

    public void executeTask(final File inputFile,
                            final LineParser<I> parser,
                            final Task<I, C> task,
                            final C runnerContext) {
        batchCount = 0;
        final long startTime = System.currentTimeMillis();
        try (LineReader<I> reader = new LineReader<>(inputFile, parser,this.skipSize)){
            List<I> inputCollection = getInputBatch(reader);

            int activeThreads = 0;

            LOG.info("Executing batches for task {}", task.getClass().getSimpleName());

            while (inputCollection.size() > 0) {
                RunnableTaskContainer<I, C> container = new RunnableTaskContainer<>(inputCollection, runnerContext, task);
                completionService.submit(container);
                inputCollection = getInputBatch(reader);
                activeThreads++;
                if (activeThreads == numThreads) {
                    processBatchResults();
                    activeThreads--;
                }
            }

            while (activeThreads > 0) {
                processBatchResults();
                activeThreads--;
            }

            LOG.info("Finished batches for task {}", task.getClass().getSimpleName());
            LOG.info("{}\t{}ms", "TOTAL TIME:", System.currentTimeMillis() - startTime);

        } catch (Exception ex) {
            LOG.error("Failed to execute task", ex);
        }

        task.postExecute(runnerContext);
        logOutputWriter.outputCounters();
        logOutputWriter.outputAggregators();
        if (alternateOutputWriter.isPresent()) {
            alternateOutputWriter.get().outputCounters();
            alternateOutputWriter.get().outputAggregators();
        }
        Counters.clearCounters();
        TaskFileWriter.closeAll();
    }

    private void processBatchResults() {
        try {
            Future<Boolean> taskFuture = completionService.take();
            batchCount++;
            Boolean result = taskFuture.get();
            if (result) {
                if (batchCount % 100 == 0) {
                    LOG.info("Batches complete {}", batchCount);
                    logOutputWriter.outputCounters();
                }
            } else {
                LOG.error("Task failed");
            }
        } catch (InterruptedException | ExecutionException ex) {
            LOG.error("An error occurred executing task", ex);
        }
    }

    private List<I> getInputBatch(LineReader<I> reader) throws Exception {
        List<I> inputCollection = new ArrayList<>(batchSize);

        I inputLine;

        while (inputCollection.size() < batchSize && inputSize > currInputSize++
                && (inputLine = reader.parseNextInputLine()) != null) {
            inputCollection.add(inputLine);
        }

        return inputCollection;
    }
}