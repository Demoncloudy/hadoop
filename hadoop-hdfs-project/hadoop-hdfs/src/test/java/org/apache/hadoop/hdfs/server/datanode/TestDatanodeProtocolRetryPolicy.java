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

package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.base.Supplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This tests DatanodeProtocol retry policy
 */
public class TestDatanodeProtocolRetryPolicy {
    private static final Log LOG = LogFactory.getLog(
            TestDatanodeProtocolRetryPolicy.class);
    private static final String DATA_DIR =
            MiniDFSCluster.getBaseDirectory() + "data";
    private final static String CLUSTER_ID = "testClusterID";
    private final static String POOL_ID = "BP-TEST";
    private final static InetSocketAddress NN_ADDR = new InetSocketAddress(
            "localhost", 5020);
    private static DatanodeRegistration datanodeRegistration =
            DFSTestUtil.getLocalDatanodeRegistration();

    static {
        ((Log4JLogger) LOG).getLogger().setLevel(Level.ALL);
    }

    ArrayList<StorageLocation> locations = new ArrayList<StorageLocation>();
    private DataNode dn;
    private Configuration conf;
    private boolean tearDownDone;

    /**
     * Starts an instance of DataNode
     *
     * @throws IOException
     */
    @Before
    public void startUp() throws IOException, URISyntaxException {
        tearDownDone = false;
        conf = new HdfsConfiguration();
        conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, DATA_DIR);
        conf.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "0.0.0.0:0");
        conf.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
        conf.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, "0.0.0.0:0");
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
        FileSystem.setDefaultUri(conf,
                "hdfs://" + NN_ADDR.getHostName() + ":" + NN_ADDR.getPort());
        File dataDir = new File(DATA_DIR);
        FileUtil.fullyDelete(dataDir);
        dataDir.mkdirs();
        StorageLocation location = StorageLocation.parse(dataDir.getPath());
        locations.add(location);
    }

    /**
     * Cleans the resources and closes the instance of datanode
     *
     * @throws IOException if an error occurred
     */
    @After
    public void tearDown() throws IOException {
        if (!tearDownDone && dn != null) {
            try {
                dn.shutdown();
            } catch (Exception e) {
                LOG.error("Cannot close: ", e);
            } finally {
                File dir = new File(DATA_DIR);
                if (dir.exists())
                    Assert.assertTrue(
                            "Cannot delete data-node dirs", FileUtil.fullyDelete(dir));
            }
            tearDownDone = true;
        }
    }

    private void waitForBlockReport(
            final DatanodeProtocolClientSideTranslatorPB mockNN) throws Exception {
        GenericTestUtils.waitFor(new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                try {
                    Mockito.verify(mockNN).blockReport(
                            Mockito.eq(datanodeRegistration),
                            Mockito.eq(POOL_ID),
                            Mockito.<StorageBlockReport[]>anyObject(),
                            Mockito.<BlockReportContext>anyObject());
                    return true;
                } catch (Throwable t) {
                    LOG.info("waiting on block report: " + t.getMessage());
                    return false;
                }
            }
        }, 500, 100000);
    }

    /**
     * Verify the following scenario.
     * 1. The initial DatanodeProtocol.registerDatanode succeeds.
     * 2. DN starts heartbeat process.
     * 3. In the first heartbeat, NN asks DN to reregister.
     * 4. DN calls DatanodeProtocol.registerDatanode.
     * 5. DatanodeProtocol.registerDatanode throws EOFException.
     * 6. DN retries.
     * 7. DatanodeProtocol.registerDatanode succeeds.
     */
    @Test(timeout = 60000)
    public void testDatanodeRegistrationRetry() throws Exception {
        final DatanodeProtocolClientSideTranslatorPB namenode =
                mock(DatanodeProtocolClientSideTranslatorPB.class);

        Mockito.doAnswer(new Answer<DatanodeRegistration>() {
            int i = 0;

            @Override
            public DatanodeRegistration answer(InvocationOnMock invocation)
                    throws Throwable {
                i++;
                if (i > 1 && i < 5) {
                    LOG.info("mockito exception " + i);
                    throw new EOFException("TestDatanodeProtocolRetryPolicy");
                } else {
                    DatanodeRegistration dr =
                            (DatanodeRegistration) invocation.getArguments()[0];
                    datanodeRegistration.setDatanodeUuidForTesting(dr.getDatanodeUuid());
                    LOG.info("mockito succeeded " + datanodeRegistration);
                    return datanodeRegistration;
                }
            }
        }).when(namenode).registerDatanode(
                Mockito.any(DatanodeRegistration.class));

        when(namenode.versionRequest()).thenReturn(
                new NamespaceInfo(1, CLUSTER_ID, POOL_ID, 1L));

        Mockito.doAnswer(new Answer<HeartbeatResponse>() {
            int i = 0;

            @Override
            public HeartbeatResponse answer(InvocationOnMock invocation)
                    throws Throwable {
                i++;
                HeartbeatResponse heartbeatResponse;
                if (i == 1) {
                    LOG.info("mockito heartbeatResponse registration " + i);
                    heartbeatResponse = new HeartbeatResponse(
                            new DatanodeCommand[]{RegisterCommand.REGISTER},
                            new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 1),
                            null);
                } else {
                    LOG.info("mockito heartbeatResponse " + i);
                    heartbeatResponse = new HeartbeatResponse(
                            new DatanodeCommand[0],
                            new NNHAStatusHeartbeat(HAServiceState.ACTIVE, 1),
                            null);
                }
                return heartbeatResponse;
            }
        }).when(namenode).sendHeartbeat(
                Mockito.any(DatanodeRegistration.class),
                Mockito.any(StorageReport[].class),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.anyInt(),
                Mockito.anyInt(),
                Mockito.anyInt());

        dn = new DataNode(conf, locations, null) {
            @Override
            DatanodeProtocolClientSideTranslatorPB connectToNN(
                    InetSocketAddress nnAddr) throws IOException {
                Assert.assertEquals(NN_ADDR, nnAddr);
                return namenode;
            }
        };

        // Trigger a heartbeat so that it acknowledges the NN as active.
        dn.getAllBpOs()[0].triggerHeartbeatForTests();

        waitForBlockReport(namenode);
    }
}
