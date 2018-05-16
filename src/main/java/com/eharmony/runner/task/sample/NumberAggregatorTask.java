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

package com.eharmony.runner.task.sample;

import com.eharmony.runner.statistics.Aggregators;
import com.eharmony.runner.statistics.Counters;
import com.eharmony.runner.task.DefaultRunnerContext;
import com.eharmony.runner.task.Task;

import java.util.Collection;

public class NumberAggregatorTask implements Task<Integer, DefaultRunnerContext> {
    @Override
    public boolean executeTask(final Collection<Integer> input, final DefaultRunnerContext runnerContext) {
        input.forEach(this::aggregateNumber);
        return true;
    }

    private void aggregateNumber(final Integer value) {
        Counters.incrementCounter("NUM_VALUES");
        Counters.incrementCounterByValue("SUM_VALUES", value);
        Aggregators.aggregate("VALUE", value);
    }
}
