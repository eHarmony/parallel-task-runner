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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class LineReader<I> implements AutoCloseable {
    private BufferedReader reader;
    private LineParser<I> lineParser;

    public LineReader(File inputFile, LineParser<I> lineParser) throws FileNotFoundException {
        if (inputFile == null) {
            throw new FileNotFoundException("No input file specified");
        }

        this.reader = new BufferedReader(new FileReader(inputFile));
        this.lineParser = lineParser;
    }

    public I parseNextInputLine() throws Exception {
        String input = reader.readLine();
        if (input != null) {
            return lineParser.parseLine(input);
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }
}
