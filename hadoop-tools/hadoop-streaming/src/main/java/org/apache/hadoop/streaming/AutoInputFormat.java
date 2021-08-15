/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.streaming;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.*;

import java.io.EOFException;
import java.io.IOException;

/**
 * An {@link InputFormat} that tries to deduce the types of the input files
 * automatically. It can currently handle text and sequence files.
 */
public class AutoInputFormat extends FileInputFormat {

    private TextInputFormat textInputFormat = new TextInputFormat();

    private SequenceFileInputFormat seqFileInputFormat =
            new SequenceFileInputFormat();

    public void configure(JobConf job) {
        textInputFormat.configure(job);
        // SequenceFileInputFormat has no configure() method
    }

    public RecordReader getRecordReader(InputSplit split, JobConf job,
                                        Reporter reporter) throws IOException {
        FileSplit fileSplit = (FileSplit) split;
        FileSystem fs = FileSystem.get(fileSplit.getPath().toUri(), job);
        FSDataInputStream is = fs.open(fileSplit.getPath());
        byte[] header = new byte[3];
        RecordReader reader = null;
        try {
            is.readFully(header);
        } catch (EOFException eof) {
            reader = textInputFormat.getRecordReader(split, job, reporter);
        } finally {
            is.close();
        }
        if (header[0] == 'S' && header[1] == 'E' && header[2] == 'Q') {
            reader = seqFileInputFormat.getRecordReader(split, job, reporter);
        } else {
            reader = textInputFormat.getRecordReader(split, job, reporter);
        }
        return reader;
    }

}
