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

package com.eharmony.runner.statistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Aggregators {
    private static ConcurrentHashMap<String, Aggregator> AGGREGATOR_MAP =
            new ConcurrentHashMap<>();

    public static void aggregate(String aggregateName, Integer value) {
        aggregate(aggregateName, new Long(value));
    }

    public static void aggregate(String aggregateName, Long value) {
        Aggregator aggregate = getAggregate(aggregateName);

        aggregate.add(value);
    }

    public static ConcurrentHashMap<String, Aggregator> getAggregators() {
        return AGGREGATOR_MAP;
    }

    private static Aggregator getAggregate(String aggregateName) {
        Aggregator aggregator = AGGREGATOR_MAP.get(aggregateName);

        if (aggregator == null) {
            aggregator = AGGREGATOR_MAP.putIfAbsent(aggregateName, new Aggregator());

            if (aggregator == null) {
                aggregator = AGGREGATOR_MAP.get(aggregateName);
            }
        }
        return aggregator;
    }

    public static class Aggregator {
        private List<Long> aggregateValues = new ArrayList<>();
        private List<Long> sortedValues;
        private Long min;
        private Long max;
        private Long sum = 0L;

        public synchronized void add(Long value) {
            if (min == null || value < min) {
                min = value;
            }
            if (max == null || value > max) {
                max = value;
            }
            aggregateValues.add(value);
            sum += value;
        }

        public Double getMean() {
            return sum / ((1d) * aggregateValues.size());
        }

        public Double getMedian() {
            if (aggregateValues.size() == 0) {
                return 0d;
            }

            final List<Long> values = getSortedValues();
            final int listSize      = values.size();
            final int middleIndex   = (listSize - 1) / 2;

            if (listSize % 2 == 0) {
                return (values.get(middleIndex) + values.get(middleIndex + 1)) / 2d;
            } else {
                return (1d) * values.get(middleIndex);
            }
        }

        public Long getMode() {
            Long modeCount = 0L;
            Long modeValue = 0L;
            Map<Long, Long> counts = new HashMap<>();

            for (Long value : getSortedValues()) {
                Long valueCount = counts.get(value);

                if (valueCount == null) {
                    valueCount = 0L;
                }

                valueCount++;

                if (valueCount > modeCount) {
                    modeValue = value;
                    modeCount = valueCount;
                }

                counts.put(value, valueCount);
            }

            return modeValue;
        }

        public Long getMin() {
            return min;
        }

        public Long getMax() {
            return max;
        }

        private List<Long> getSortedValues() {
            if (sortedValues == null) {
                sortedValues = Arrays.asList(new Long[aggregateValues.size()]);
                Collections.copy(sortedValues, aggregateValues);
                Collections.sort(sortedValues);
            }
            return sortedValues;
        }
    }

}
