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

import java.lang.reflect.ParameterizedType;
import java.util.Collection;

public interface Task<I, C extends RunnerContext> {
    boolean executeTask(Collection<I> input, C runnerContext);

    default Class<C> getRunnerContextClass() {
        return (Class<C>)((ParameterizedType) getClass().getGenericInterfaces()[0]).getActualTypeArguments()[1];
    }

    default void postExecute(C runnerContext) {
        // Do Nothing
    }
}
