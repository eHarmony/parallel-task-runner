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

package com.eharmony.runner.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class TaskFileWriter implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(TaskFileWriter.class);
    private static final String FILE_FOLDER = "output-files/";
    private static final ConcurrentHashMap<String, TaskFileWriter> FILE_WRITERS = new ConcurrentHashMap<>();

    private final String fileName;
    private final boolean append;
    private BufferedWriter writer;

    private TaskFileWriter(String fileName, boolean append) throws IOException {
        this.fileName = FILE_FOLDER + fileName;
        this.append = append;
        File file = new File(this.fileName);
        if (!file.createNewFile() && !file.canWrite()) {
            throw new IOException("Unable to write to file " + this.fileName);
        }
    }

    public synchronized void write(String line) throws IOException {
        if (writer == null) {
            initializeWriter();
        }
        writer.write(line);
    }

    private void initializeWriter() throws IOException {
        writer = new BufferedWriter(new FileWriter(fileName, append));
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
    public static TaskFileWriter getFileWriter(String fileName, boolean append) throws IOException {
        if (FILE_WRITERS.contains(fileName)) {
            return FILE_WRITERS.get(fileName);
        } else {
            TaskFileWriter fileWriter = new TaskFileWriter(fileName, append);
            TaskFileWriter existingWriter = FILE_WRITERS.putIfAbsent(fileName, fileWriter);
            if (existingWriter == null) {
                return fileWriter;
            } else {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    LOG.error("Failed to close file", e);
                }
                return existingWriter;
            }
        }

    }

    public static TaskFileWriter getFileWriter(String fileName) throws IOException {
        return getFileWriter(fileName, false);
    }

    public static void closeAll() {
        for (TaskFileWriter fileWriter: FILE_WRITERS.values()) {
            try {
                fileWriter.close();
            } catch (IOException e) {
                LOG.error("Failed to close file", e);
            }
        }
    }
}
