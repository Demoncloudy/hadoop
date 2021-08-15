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

package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Input format that is a <code>CombineFileInputFormat</code>-equivalent for
 * <code>SequenceFileInputFormat</code>.
 *
 * @see CombineFileInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineSequenceFileInputFormat<K, V>
        extends CombineFileInputFormat<K, V> {
    @SuppressWarnings({"rawtypes", "unchecked"})
    public RecordReader<K, V> getRecordReader(InputSplit split, JobConf conf,
                                              Reporter reporter) throws IOException {
        return new CombineFileRecordReader(conf, (CombineFileSplit) split, reporter,
                SequenceFileRecordReaderWrapper.class);
    }

    /**
     * A record reader that may be passed to <code>CombineFileRecordReader</code>
     * so that it can be used in a <code>CombineFileInputFormat</code>-equivalent
     * for <code>SequenceFileInputFormat</code>.
     *
     * @see CombineFileRecordReader
     * @see CombineFileInputFormat
     * @see SequenceFileInputFormat
     */
    private static class SequenceFileRecordReaderWrapper<K, V>
            extends CombineFileRecordReaderWrapper<K, V> {
        // this constructor signature is required by CombineFileRecordReader
        public SequenceFileRecordReaderWrapper(CombineFileSplit split,
                                               Configuration conf, Reporter reporter, Integer idx) throws IOException {
            super(new SequenceFileInputFormat<K, V>(), split, conf, reporter, idx);
        }
    }
}
