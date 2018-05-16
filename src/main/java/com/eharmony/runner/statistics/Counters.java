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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Counters {
    private static ConcurrentHashMap<String, AtomicLong> COUNTER_MAP = new ConcurrentHashMap<>();

    public static void incrementCounter(final String counterName) {
        AtomicLong count = getCounter(counterName);
        count.incrementAndGet();
    }

    public static void incrementCounterByValue(final String counterName, final int value) {
        AtomicLong count = getCounter(counterName);

        count.getAndAdd(value);
    }

    private static AtomicLong getCounter(final String counterName) {
        AtomicLong count = COUNTER_MAP.get(counterName);

        if (count == null) {
            count = COUNTER_MAP.putIfAbsent(counterName, new AtomicLong(0));

            if (count == null) {
                count = COUNTER_MAP.get(counterName);
            }
        }
        return count;
    }

    public static ConcurrentHashMap<String, AtomicLong> getCounters() {
        return new ConcurrentHashMap<>(COUNTER_MAP);
    }

    public static void clearCounters() {
        COUNTER_MAP.clear();
    }
}
