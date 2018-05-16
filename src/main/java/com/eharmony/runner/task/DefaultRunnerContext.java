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
import com.google.common.collect.Sets;

import java.util.Properties;
import java.util.Set;

/**
 * Default implementation of @see {@link com.eharmony.runner.RunnerContext}. This is meant to be used for tasks that
 * do not require any properties outside of the default properties.
 */
public class DefaultRunnerContext implements RunnerContext {
    @Override
    public void init(final Properties properties) throws Exception {
        // Does not have any initialization properties
    }

    @Override
    public Set<String> getRequiredPropertyNames() {
        return Sets.newHashSet();
    }
}
