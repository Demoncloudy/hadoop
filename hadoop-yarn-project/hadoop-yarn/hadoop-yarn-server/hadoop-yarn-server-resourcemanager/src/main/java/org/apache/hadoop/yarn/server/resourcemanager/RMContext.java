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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.*;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

/**
 * Context of the ResourceManager.
 */
public interface RMContext {

    Dispatcher getDispatcher();

    boolean isHAEnabled();

    HAServiceState getHAServiceState();

    RMStateStore getStateStore();

    ConcurrentMap<ApplicationId, RMApp> getRMApps();

    ConcurrentMap<ApplicationId, ByteBuffer> getSystemCredentialsForApps();

    ConcurrentMap<String, RMNode> getInactiveRMNodes();

    ConcurrentMap<NodeId, RMNode> getRMNodes();

    AMLivelinessMonitor getAMLivelinessMonitor();

    AMLivelinessMonitor getAMFinishingMonitor();

    ContainerAllocationExpirer getContainerAllocationExpirer();

    DelegationTokenRenewer getDelegationTokenRenewer();

    AMRMTokenSecretManager getAMRMTokenSecretManager();

    RMContainerTokenSecretManager getContainerTokenSecretManager();

    NMTokenSecretManagerInRM getNMTokenSecretManager();

    ResourceScheduler getScheduler();

    NodesListManager getNodesListManager();

    ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager();

    AdminService getRMAdminService();

    ClientRMService getClientRMService();

    void setClientRMService(ClientRMService clientRMService);

    ApplicationMasterService getApplicationMasterService();

    ResourceTrackerService getResourceTrackerService();

    RMDelegationTokenSecretManager getRMDelegationTokenSecretManager();

    void setRMDelegationTokenSecretManager(
            RMDelegationTokenSecretManager delegationTokenSecretManager);

    RMApplicationHistoryWriter getRMApplicationHistoryWriter();

    void setRMApplicationHistoryWriter(
            RMApplicationHistoryWriter rmApplicationHistoryWriter);

    SystemMetricsPublisher getSystemMetricsPublisher();

    void setSystemMetricsPublisher(SystemMetricsPublisher systemMetricsPublisher);

    ConfigurationProvider getConfigurationProvider();

    boolean isWorkPreservingRecoveryEnabled();

    RMNodeLabelsManager getNodeLabelManager();

    public void setNodeLabelManager(RMNodeLabelsManager mgr);

    long getEpoch();

    ReservationSystem getReservationSystem();

    boolean isSchedulerReadyForAllocatingContainers();

    Configuration getYarnConfiguration();
}
