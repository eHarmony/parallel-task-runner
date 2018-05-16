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

package com.eharmony.runner.output;

import com.eharmony.runner.file.TaskFileWriter;
import com.eharmony.runner.statistics.Aggregators;
import com.eharmony.runner.statistics.Counters;
import com.eharmony.runner.task.TaskRunner;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CsvStatisticsOutputWriter implements StatisticsOutputWriter {
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("ddMMyyyy-HHmmss");
    private static final Logger LOG = LoggerFactory.getLogger(TaskRunner.class);

    @Override
    public void outputCounters() {
        try (TaskFileWriter fileWriter =
                     TaskFileWriter.getFileWriter(DATE_FORMAT.print(DateTime.now()) + ".counters.csv")) {
            List<Map.Entry<String, AtomicLong>> counters = new ArrayList<>(Counters.getCounters().entrySet());

            if (counters.size() == 0) {
                LOG.warn("No counters found, no counters.csv will be created.");
                return;
            }

            fileWriter.write("COUNTER_NAME,COUNTER_VALUE\n");
            Collections.sort(counters, (thisEntry, thatEntry) -> thisEntry.getKey().compareTo(thatEntry.getKey()));

            for (Map.Entry<String, AtomicLong> counter : counters) {
                fileWriter.write(String.format("%s,%d\n", counter.getKey(), counter.getValue().get()));
            }
            fileWriter.close();
        } catch (IOException ex) {
            LOG.error("Unable to write to counters.csv file.", ex);
        }
    }

    @Override
    public void outputAggregators() {
        try (TaskFileWriter fileWriter =
                    TaskFileWriter.getFileWriter(DATE_FORMAT.print(DateTime.now()) + ".aggregators.csv")) {
            List<Map.Entry<String, Aggregators.Aggregator>> aggregators =
                    new ArrayList<>(Aggregators.getAggregators().entrySet());

            if (aggregators.size() == 0) {
                LOG.warn("No aggregators found, no aggregators.csv will be created.");
                return;
            }

            fileWriter.write("AGGREGATOR_NAME,MEAN,MEDIAN,MODE,MIN,MAX\n");
            Collections.sort(aggregators, (thisEntry, thatEntry) -> thisEntry.getKey().compareTo(thatEntry.getKey()));

            for (Map.Entry<String, Aggregators.Aggregator> aggregator : aggregators) {
                fileWriter.write(String.format("%s,", aggregator.getKey()));
                fileWriter.write(String.format("%s,",
                        LogStatisticsOutputWriter.truncateDecimal(aggregator.getValue().getMean())));
                fileWriter.write(String.format("%s,",
                        LogStatisticsOutputWriter.truncateDecimal(aggregator.getValue().getMedian())));
                fileWriter.write(String.format("%d,", aggregator.getValue().getMode()));
                fileWriter.write(String.format("%d,", aggregator.getValue().getMin()));
                fileWriter.write(String.format("%d\n", aggregator.getValue().getMax()));
            }
        } catch (IOException ex) {
            LOG.error("Unable to write to aggregators.csv file.", ex);
        }
    }
}
