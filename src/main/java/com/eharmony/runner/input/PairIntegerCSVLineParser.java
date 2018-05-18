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

package com.eharmony.runner.input;

import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PairIntegerCSVLineParser implements LineParser {

    private static final Logger LOG = LoggerFactory.getLogger(PairIntegerCSVLineParser.class);

    @Override
    public Object parseLine(final String input) throws Exception {
        String[] pair = input.split(",");
        try {
            return new Pair<>(Integer.parseInt(pair[0]), Integer.parseInt(pair[1]));
        }catch(NumberFormatException | ArrayIndexOutOfBoundsException e){
            LOG.error("Unable to parse line" + input);
            return new Pair<>(-1,-1);
        }
    }
}
