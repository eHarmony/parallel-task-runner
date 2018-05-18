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

import com.eharmony.runner.statistics.Aggregators;
import com.eharmony.runner.statistics.Counters;
import com.eharmony.runner.task.TaskRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class LogStatisticsOutputWriter implements StatisticsOutputWriter {
    private static final int PRINT_PADDING = 30;
    private static final Logger LOG = LoggerFactory.getLogger(TaskRunner.class);

    @Override
    public void outputCounters() {
        List<Map.Entry<String, AtomicLong>> counters = new ArrayList<>(Counters.getCounters().entrySet());

        if (counters.size() == 0) {
            return;
        }

        LOG.info("========================================================");
        LOG.info("Counters");
        LOG.info("========================================================");

        Collections.sort(counters, (thisEntry, thatEntry) -> thisEntry.getKey().compareTo(thatEntry.getKey()));

        for (Map.Entry<String, AtomicLong> counter : counters) {
            LOG.info("{}\t{}", normalizeCounterName(counter.getKey()), counter.getValue());
        }
        LOG.info("========================================================");
    }

    @Override
    public void outputAggregators() {
        List<Map.Entry<String, Aggregators.Aggregator>> aggregators =
                new ArrayList<>(Aggregators.getAggregators().entrySet());

        if (aggregators.size() == 0) {
            return;
        }

        LOG.info("========================================================");
        LOG.info("Aggregates");
        LOG.info("========================================================");

        Collections.sort(aggregators, (thisEntry, thatEntry) -> thisEntry.getKey().compareTo(thatEntry.getKey()));

        for (Map.Entry<String, Aggregators.Aggregator> aggregator : aggregators) {
            LOG.info("--{}", aggregator.getKey());
            LOG.info("{}\t{}", normalizeCounterName("MEAN"), truncateDecimal(aggregator.getValue().getMean()));
            LOG.info("{}\t{}", normalizeCounterName("MEDIAN"), truncateDecimal(aggregator.getValue().getMedian()));
            LOG.info("{}\t{}", normalizeCounterName("MODE"), aggregator.getValue().getMode());
            LOG.info("{}\t{}", normalizeCounterName("MIN"), aggregator.getValue().getMin());
            LOG.info("{}\t{}", normalizeCounterName("MAX"), aggregator.getValue().getMax());
        }
        LOG.info("========================================================");
    }

    public static String truncateDecimal(double value) {
        if (value - Math.floor(value) == 0) {
            return new DecimalFormat("#").format(value);
        }

        return new DecimalFormat("#.##").format(value);
    }

    private String normalizeCounterName(String counterName) {
        if (counterName.length() < PRINT_PADDING) {
            StringBuilder builder = new StringBuilder(counterName).append(":");
            for (int i = counterName.length(); i < PRINT_PADDING; i++) {
                builder.append(" ");
            }
            return builder.toString();
        } else {
            return counterName + ":";
        }
    }
}
