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

package org.apache.hadoop.yarn.server.api.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.*;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.*;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.*;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

@Private
public class ResourceManagerAdministrationProtocolPBClientImpl implements ResourceManagerAdministrationProtocol, Closeable {

    private ResourceManagerAdministrationProtocolPB proxy;

    public ResourceManagerAdministrationProtocolPBClientImpl(long clientVersion, InetSocketAddress addr,
                                                             Configuration conf) throws IOException {
        RPC.setProtocolEngine(conf, ResourceManagerAdministrationProtocolPB.class,
                ProtobufRpcEngine.class);
        proxy = (ResourceManagerAdministrationProtocolPB) RPC.getProxy(
                ResourceManagerAdministrationProtocolPB.class, clientVersion, addr, conf);
    }

    @Override
    public void close() {
        if (this.proxy != null) {
            RPC.stopProxy(this.proxy);
        }
    }

    @Override
    public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
            throws YarnException, IOException {
        RefreshQueuesRequestProto requestProto =
                ((RefreshQueuesRequestPBImpl) request).getProto();
        try {
            return new RefreshQueuesResponsePBImpl(
                    proxy.refreshQueues(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
            throws YarnException, IOException {
        RefreshNodesRequestProto requestProto =
                ((RefreshNodesRequestPBImpl) request).getProto();
        try {
            return new RefreshNodesResponsePBImpl(
                    proxy.refreshNodes(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
            RefreshSuperUserGroupsConfigurationRequest request)
            throws YarnException, IOException {
        RefreshSuperUserGroupsConfigurationRequestProto requestProto =
                ((RefreshSuperUserGroupsConfigurationRequestPBImpl) request).getProto();
        try {
            return new RefreshSuperUserGroupsConfigurationResponsePBImpl(
                    proxy.refreshSuperUserGroupsConfiguration(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
            RefreshUserToGroupsMappingsRequest request) throws YarnException,
            IOException {
        RefreshUserToGroupsMappingsRequestProto requestProto =
                ((RefreshUserToGroupsMappingsRequestPBImpl) request).getProto();
        try {
            return new RefreshUserToGroupsMappingsResponsePBImpl(
                    proxy.refreshUserToGroupsMappings(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public RefreshAdminAclsResponse refreshAdminAcls(
            RefreshAdminAclsRequest request) throws YarnException, IOException {
        RefreshAdminAclsRequestProto requestProto =
                ((RefreshAdminAclsRequestPBImpl) request).getProto();
        try {
            return new RefreshAdminAclsResponsePBImpl(
                    proxy.refreshAdminAcls(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public RefreshServiceAclsResponse refreshServiceAcls(
            RefreshServiceAclsRequest request) throws YarnException,
            IOException {
        RefreshServiceAclsRequestProto requestProto =
                ((RefreshServiceAclsRequestPBImpl) request).getProto();
        try {
            return new RefreshServiceAclsResponsePBImpl(proxy.refreshServiceAcls(
                    null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public String[] getGroupsForUser(String user) throws IOException {
        GetGroupsForUserRequestProto requestProto =
                GetGroupsForUserRequestProto.newBuilder().setUser(user).build();
        try {
            GetGroupsForUserResponseProto responseProto =
                    proxy.getGroupsForUser(null, requestProto);
            return (String[]) responseProto.getGroupsList().toArray(
                    new String[responseProto.getGroupsCount()]);
        } catch (ServiceException e) {
            throw ProtobufHelper.getRemoteException(e);
        }
    }

    @Override
    public UpdateNodeResourceResponse updateNodeResource(
            UpdateNodeResourceRequest request) throws YarnException, IOException {
        UpdateNodeResourceRequestProto requestProto =
                ((UpdateNodeResourceRequestPBImpl) request).getProto();
        try {
            return new UpdateNodeResourceResponsePBImpl(proxy.updateNodeResource(null,
                    requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public AddToClusterNodeLabelsResponse addToClusterNodeLabels(
            AddToClusterNodeLabelsRequest request) throws YarnException, IOException {
        AddToClusterNodeLabelsRequestProto requestProto =
                ((AddToClusterNodeLabelsRequestPBImpl) request).getProto();
        try {
            return new AddToClusterNodeLabelsResponsePBImpl(
                    proxy.addToClusterNodeLabels(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
            RemoveFromClusterNodeLabelsRequest request) throws YarnException,
            IOException {
        RemoveFromClusterNodeLabelsRequestProto requestProto =
                ((RemoveFromClusterNodeLabelsRequestPBImpl) request).getProto();
        try {
            return new RemoveFromClusterNodeLabelsResponsePBImpl(
                    proxy.removeFromClusterNodeLabels(null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }

    @Override
    public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(
            ReplaceLabelsOnNodeRequest request) throws YarnException, IOException {
        ReplaceLabelsOnNodeRequestProto requestProto =
                ((ReplaceLabelsOnNodeRequestPBImpl) request).getProto();
        try {
            return new ReplaceLabelsOnNodeResponsePBImpl(proxy.replaceLabelsOnNodes(
                    null, requestProto));
        } catch (ServiceException e) {
            RPCUtil.unwrapAndThrowException(e);
            return null;
        }
    }
}
