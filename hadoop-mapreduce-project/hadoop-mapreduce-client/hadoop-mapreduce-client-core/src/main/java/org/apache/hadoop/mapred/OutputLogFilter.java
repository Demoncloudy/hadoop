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

package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * This class filters log files from directory given
 * It doesnt accept paths having _logs.
 * This can be used to list paths of output directory as follows:
 * Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
 * new OutputLogFilter()));
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class OutputLogFilter implements PathFilter {
    private static final PathFilter LOG_FILTER =
            new Utils.OutputFileUtils.OutputLogFilter();

    public boolean accept(Path path) {
        return LOG_FILTER.accept(path);
    }
}
