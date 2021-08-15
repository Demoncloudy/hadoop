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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
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
import org.apache.hadoop.yarn.util.Clock;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

public class RMContextImpl implements RMContext {

    private Dispatcher rmDispatcher;

    private boolean isHAEnabled;

    private HAServiceState haServiceState =
            HAServiceProtocol.HAServiceState.INITIALIZING;

    private AdminService adminService;

    private ConfigurationProvider configurationProvider;

    private RMActiveServiceContext activeServiceContext;

    private Configuration yarnConfiguration;

    private RMApplicationHistoryWriter rmApplicationHistoryWriter;
    private SystemMetricsPublisher systemMetricsPublisher;

    /**
     * Default constructor. To be used in conjunction with setter methods for
     * individual fields.
     */
    public RMContextImpl() {

    }

    @VisibleForTesting
    // helper constructor for tests
    public RMContextImpl(Dispatcher rmDispatcher,
                         ContainerAllocationExpirer containerAllocationExpirer,
                         AMLivelinessMonitor amLivelinessMonitor,
                         AMLivelinessMonitor amFinishingMonitor,
                         DelegationTokenRenewer delegationTokenRenewer,
                         AMRMTokenSecretManager appTokenSecretManager,
                         RMContainerTokenSecretManager containerTokenSecretManager,
                         NMTokenSecretManagerInRM nmTokenSecretManager,
                         ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager,
                         ResourceScheduler scheduler) {
        this();
        this.setDispatcher(rmDispatcher);
        setActiveServiceContext(new RMActiveServiceContext(rmDispatcher,
                containerAllocationExpirer, amLivelinessMonitor, amFinishingMonitor,
                delegationTokenRenewer, appTokenSecretManager,
                containerTokenSecretManager, nmTokenSecretManager,
                clientToAMTokenSecretManager,
                scheduler));

        ConfigurationProvider provider = new LocalConfigurationProvider();
        setConfigurationProvider(provider);
    }

    @VisibleForTesting
    // helper constructor for tests
    public RMContextImpl(Dispatcher rmDispatcher,
                         ContainerAllocationExpirer containerAllocationExpirer,
                         AMLivelinessMonitor amLivelinessMonitor,
                         AMLivelinessMonitor amFinishingMonitor,
                         DelegationTokenRenewer delegationTokenRenewer,
                         AMRMTokenSecretManager appTokenSecretManager,
                         RMContainerTokenSecretManager containerTokenSecretManager,
                         NMTokenSecretManagerInRM nmTokenSecretManager,
                         ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager) {
        this(
                rmDispatcher,
                containerAllocationExpirer,
                amLivelinessMonitor,
                amFinishingMonitor,
                delegationTokenRenewer,
                appTokenSecretManager,
                containerTokenSecretManager,
                nmTokenSecretManager,
                clientToAMTokenSecretManager, null);
    }

    @Override
    public Dispatcher getDispatcher() {
        return this.rmDispatcher;
    }

    void setDispatcher(Dispatcher dispatcher) {
        this.rmDispatcher = dispatcher;
    }

    @Override
    public RMStateStore getStateStore() {
        return activeServiceContext.getStateStore();
    }

    @VisibleForTesting
    public void setStateStore(RMStateStore store) {
        activeServiceContext.setStateStore(store);
    }

    @Override
    public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
        return activeServiceContext.getRMApps();
    }

    @Override
    public ConcurrentMap<NodeId, RMNode> getRMNodes() {
        return activeServiceContext.getRMNodes();
    }

    @Override
    public ConcurrentMap<String, RMNode> getInactiveRMNodes() {
        return activeServiceContext.getInactiveRMNodes();
    }

    @Override
    public ContainerAllocationExpirer getContainerAllocationExpirer() {
        return activeServiceContext.getContainerAllocationExpirer();
    }

    void setContainerAllocationExpirer(
            ContainerAllocationExpirer containerAllocationExpirer) {
        activeServiceContext
                .setContainerAllocationExpirer(containerAllocationExpirer);
    }

    @Override
    public AMLivelinessMonitor getAMLivelinessMonitor() {
        return activeServiceContext.getAMLivelinessMonitor();
    }

    void setAMLivelinessMonitor(AMLivelinessMonitor amLivelinessMonitor) {
        activeServiceContext.setAMLivelinessMonitor(amLivelinessMonitor);
    }

    @Override
    public AMLivelinessMonitor getAMFinishingMonitor() {
        return activeServiceContext.getAMFinishingMonitor();
    }

    void setAMFinishingMonitor(AMLivelinessMonitor amFinishingMonitor) {
        activeServiceContext.setAMFinishingMonitor(amFinishingMonitor);
    }

    @Override
    public DelegationTokenRenewer getDelegationTokenRenewer() {
        return activeServiceContext.getDelegationTokenRenewer();
    }

    void setDelegationTokenRenewer(DelegationTokenRenewer delegationTokenRenewer) {
        activeServiceContext.setDelegationTokenRenewer(delegationTokenRenewer);
    }

    @Override
    public AMRMTokenSecretManager getAMRMTokenSecretManager() {
        return activeServiceContext.getAMRMTokenSecretManager();
    }

    void setAMRMTokenSecretManager(AMRMTokenSecretManager amRMTokenSecretManager) {
        activeServiceContext.setAMRMTokenSecretManager(amRMTokenSecretManager);
    }

    @Override
    public RMContainerTokenSecretManager getContainerTokenSecretManager() {
        return activeServiceContext.getContainerTokenSecretManager();
    }

    void setContainerTokenSecretManager(
            RMContainerTokenSecretManager containerTokenSecretManager) {
        activeServiceContext
                .setContainerTokenSecretManager(containerTokenSecretManager);
    }

    @Override
    public NMTokenSecretManagerInRM getNMTokenSecretManager() {
        return activeServiceContext.getNMTokenSecretManager();
    }

    void setNMTokenSecretManager(NMTokenSecretManagerInRM nmTokenSecretManager) {
        activeServiceContext.setNMTokenSecretManager(nmTokenSecretManager);
    }

    @Override
    public ResourceScheduler getScheduler() {
        return activeServiceContext.getScheduler();
    }

    void setScheduler(ResourceScheduler scheduler) {
        activeServiceContext.setScheduler(scheduler);
    }

    @Override
    public ReservationSystem getReservationSystem() {
        return activeServiceContext.getReservationSystem();
    }

    void setReservationSystem(ReservationSystem reservationSystem) {
        activeServiceContext.setReservationSystem(reservationSystem);
    }

    @Override
    public NodesListManager getNodesListManager() {
        return activeServiceContext.getNodesListManager();
    }

    void setNodesListManager(NodesListManager nodesListManager) {
        activeServiceContext.setNodesListManager(nodesListManager);
    }

    @Override
    public ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager() {
        return activeServiceContext.getClientToAMTokenSecretManager();
    }

    void setClientToAMTokenSecretManager(
            ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager) {
        activeServiceContext
                .setClientToAMTokenSecretManager(clientToAMTokenSecretManager);
    }

    @Override
    public AdminService getRMAdminService() {
        return this.adminService;
    }

    void setRMAdminService(AdminService adminService) {
        this.adminService = adminService;
    }

    @Override
    public ClientRMService getClientRMService() {
        return activeServiceContext.getClientRMService();
    }

    @Override
    public void setClientRMService(ClientRMService clientRMService) {
        activeServiceContext.setClientRMService(clientRMService);
    }

    @Override
    public ApplicationMasterService getApplicationMasterService() {
        return activeServiceContext.getApplicationMasterService();
    }

    void setApplicationMasterService(
            ApplicationMasterService applicationMasterService) {
        activeServiceContext.setApplicationMasterService(applicationMasterService);
    }

    @Override
    public ResourceTrackerService getResourceTrackerService() {
        return activeServiceContext.getResourceTrackerService();
    }

    void setResourceTrackerService(ResourceTrackerService resourceTrackerService) {
        activeServiceContext.setResourceTrackerService(resourceTrackerService);
    }

    @Override
    public RMDelegationTokenSecretManager getRMDelegationTokenSecretManager() {
        return activeServiceContext.getRMDelegationTokenSecretManager();
    }

    @Override
    public void setRMDelegationTokenSecretManager(
            RMDelegationTokenSecretManager delegationTokenSecretManager) {
        activeServiceContext
                .setRMDelegationTokenSecretManager(delegationTokenSecretManager);
    }

    @Override
    public boolean isHAEnabled() {
        return isHAEnabled;
    }

    void setHAEnabled(boolean isHAEnabled) {
        this.isHAEnabled = isHAEnabled;
    }

    @Override
    public HAServiceState getHAServiceState() {
        synchronized (haServiceState) {
            return haServiceState;
        }
    }

    void setHAServiceState(HAServiceState haServiceState) {
        synchronized (haServiceState) {
            this.haServiceState = haServiceState;
        }
    }

    @Override
    public boolean isWorkPreservingRecoveryEnabled() {
        return activeServiceContext.isWorkPreservingRecoveryEnabled();
    }

    public void setWorkPreservingRecoveryEnabled(boolean enabled) {
        activeServiceContext.setWorkPreservingRecoveryEnabled(enabled);
    }

    @Override
    public RMApplicationHistoryWriter getRMApplicationHistoryWriter() {
        return this.rmApplicationHistoryWriter;
    }

    @Override
    public void setRMApplicationHistoryWriter(
            RMApplicationHistoryWriter rmApplicationHistoryWriter) {
        this.rmApplicationHistoryWriter = rmApplicationHistoryWriter;

    }

    @Override
    public SystemMetricsPublisher getSystemMetricsPublisher() {
        return this.systemMetricsPublisher;
    }

    @Override
    public void setSystemMetricsPublisher(
            SystemMetricsPublisher systemMetricsPublisher) {
        this.systemMetricsPublisher = systemMetricsPublisher;
    }

    @Override
    public ConfigurationProvider getConfigurationProvider() {
        return this.configurationProvider;
    }

    public void setConfigurationProvider(
            ConfigurationProvider configurationProvider) {
        this.configurationProvider = configurationProvider;
    }

    @Override
    public long getEpoch() {
        return activeServiceContext.getEpoch();
    }

    void setEpoch(long epoch) {
        activeServiceContext.setEpoch(epoch);
    }

    @Override
    public RMNodeLabelsManager getNodeLabelManager() {
        return activeServiceContext.getNodeLabelManager();
    }

    @Override
    public void setNodeLabelManager(RMNodeLabelsManager mgr) {
        activeServiceContext.setNodeLabelManager(mgr);
    }

    public void setSchedulerRecoveryStartAndWaitTime(long waitTime) {
        activeServiceContext.setSchedulerRecoveryStartAndWaitTime(waitTime);
    }

    public boolean isSchedulerReadyForAllocatingContainers() {
        return activeServiceContext.isSchedulerReadyForAllocatingContainers();
    }

    @Private
    @VisibleForTesting
    public void setSystemClock(Clock clock) {
        activeServiceContext.setSystemClock(clock);
    }

    public ConcurrentMap<ApplicationId, ByteBuffer> getSystemCredentialsForApps() {
        return activeServiceContext.getSystemCredentialsForApps();
    }

    @Private
    @Unstable
    public RMActiveServiceContext getActiveServiceContext() {
        return activeServiceContext;
    }

    @Private
    @Unstable
    void setActiveServiceContext(RMActiveServiceContext activeServiceContext) {
        this.activeServiceContext = activeServiceContext;
    }

    @Override
    public Configuration getYarnConfiguration() {
        return this.yarnConfiguration;
    }

    public void setYarnConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }
}
