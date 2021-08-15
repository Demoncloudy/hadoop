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
package org.apache.hadoop.tracing;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.util.LinkedList;
import java.util.List;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class SpanReceiverInfo {
    final List<ConfigurationPair> configPairs =
            new LinkedList<ConfigurationPair>();
    private final long id;
    private final String className;

    SpanReceiverInfo(long id, String className) {
        this.id = id;
        this.className = className;
    }

    public long getId() {
        return id;
    }

    public String getClassName() {
        return className;
    }

    static class ConfigurationPair {
        private final String key;
        private final String value;

        ConfigurationPair(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }
    }
}
