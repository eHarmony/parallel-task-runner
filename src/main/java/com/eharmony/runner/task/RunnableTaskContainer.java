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

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.Callable;

public class RunnableTaskContainer<I, C extends RunnerContext> implements Callable<Boolean> {
    private Collection<I> batchInput;
    private C runnerContext;
    private Task<I, C> task;

    public RunnableTaskContainer(Collection<I> batchInput, C runnerContext, Task<I, C> task) {
        this.runnerContext = Objects.requireNonNull(runnerContext);
        this.task = Objects.requireNonNull(task);
        this.batchInput = Objects.requireNonNull(batchInput);
    }

    @Override
    public Boolean call() throws Exception {
        return task.executeTask(batchInput, runnerContext);
    }
}
