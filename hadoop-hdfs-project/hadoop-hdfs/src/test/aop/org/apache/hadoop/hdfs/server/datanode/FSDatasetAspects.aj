/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fi.ProbabilityModel;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;


/**
 * This aspect takes care about faults injected into datanode.FSDatase class
 */
public aspect FSDatasetAspects {
    public static final Log LOG = LogFactory.getLog(BlockReceiverAspects.class);

    pointcut execGetBlockFile():
            // the following will inject faults inside of the method in question
            execution (* FSDataset.getBlockFile(..)) && !within(FSDatasetAspects +);

    pointcut callCreateBlockWriteStream(ReplicaInPipeline repl):
            call (BlockWriteStreams createStreams(..))
                    && target (repl)
                    && !within(FSDatasetAspects +);

    // This aspect specifies the logic of our fault point.
    // In this case it simply throws DiskErrorException at the very beginning of
    // invocation of the method, specified by callGetBlockFile() pointcut
    before()throws DiskErrorException: execGetBlockFile() {
        if (ProbabilityModel.injectCriteria(FSDataset.class.getSimpleName())) {
            LOG.info("Before the injection point");
            Thread.dumpStack();
            throw new DiskErrorException("FI: injected fault point at "
                    + thisJoinPoint.getStaticPart().getSourceLocation());
        }
    }

    before(ReplicaInPipeline repl)throws DiskOutOfSpaceException: callCreateBlockWriteStream(repl) {
        if (ProbabilityModel.injectCriteria(FSDataset.class.getSimpleName())) {
            LOG.info("Before the injection point");
            Thread.dumpStack();
            throw new DiskOutOfSpaceException("FI: injected fault point at "
                    + thisJoinPoint.getStaticPart().getSourceLocation());
        }
    }
}
