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

package org.apache.hadoop.yarn.server.api.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.*;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocolPB;
import org.apache.hadoop.yarn.server.api.protocolrecords.*;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.*;

import java.io.IOException;

@Private
public class ResourceManagerAdministrationProtocolPBServiceImpl implements ResourceManagerAdministrationProtocolPB {

    private ResourceManagerAdministrationProtocol real;

    public ResourceManagerAdministrationProtocolPBServiceImpl(ResourceManagerAdministrationProtocol impl) {
        this.real = impl;
    }

    @Override
    public RefreshQueuesResponseProto refreshQueues(RpcController controller,
                                                    RefreshQueuesRequestProto proto) throws ServiceException {
        RefreshQueuesRequestPBImpl request = new RefreshQueuesRequestPBImpl(proto);
        try {
            RefreshQueuesResponse response = real.refreshQueues(request);
            return ((RefreshQueuesResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public RefreshAdminAclsResponseProto refreshAdminAcls(
            RpcController controller, RefreshAdminAclsRequestProto proto)
            throws ServiceException {
        RefreshAdminAclsRequestPBImpl request =
                new RefreshAdminAclsRequestPBImpl(proto);
        try {
            RefreshAdminAclsResponse response = real.refreshAdminAcls(request);
            return ((RefreshAdminAclsResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public RefreshNodesResponseProto refreshNodes(RpcController controller,
                                                  RefreshNodesRequestProto proto) throws ServiceException {
        RefreshNodesRequestPBImpl request = new RefreshNodesRequestPBImpl(proto);
        try {
            RefreshNodesResponse response = real.refreshNodes(request);
            return ((RefreshNodesResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public RefreshSuperUserGroupsConfigurationResponseProto
    refreshSuperUserGroupsConfiguration(
            RpcController controller,
            RefreshSuperUserGroupsConfigurationRequestProto proto)
            throws ServiceException {
        RefreshSuperUserGroupsConfigurationRequestPBImpl request =
                new RefreshSuperUserGroupsConfigurationRequestPBImpl(proto);
        try {
            RefreshSuperUserGroupsConfigurationResponse response =
                    real.refreshSuperUserGroupsConfiguration(request);
            return ((RefreshSuperUserGroupsConfigurationResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public RefreshUserToGroupsMappingsResponseProto refreshUserToGroupsMappings(
            RpcController controller, RefreshUserToGroupsMappingsRequestProto proto)
            throws ServiceException {
        RefreshUserToGroupsMappingsRequestPBImpl request =
                new RefreshUserToGroupsMappingsRequestPBImpl(proto);
        try {
            RefreshUserToGroupsMappingsResponse response =
                    real.refreshUserToGroupsMappings(request);
            return ((RefreshUserToGroupsMappingsResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public RefreshServiceAclsResponseProto refreshServiceAcls(
            RpcController controller, RefreshServiceAclsRequestProto proto)
            throws ServiceException {
        RefreshServiceAclsRequestPBImpl request =
                new RefreshServiceAclsRequestPBImpl(proto);
        try {
            RefreshServiceAclsResponse response =
                    real.refreshServiceAcls(request);
            return ((RefreshServiceAclsResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetGroupsForUserResponseProto getGroupsForUser(
            RpcController controller, GetGroupsForUserRequestProto request)
            throws ServiceException {
        String user = request.getUser();
        try {
            String[] groups = real.getGroupsForUser(user);
            GetGroupsForUserResponseProto.Builder responseBuilder =
                    GetGroupsForUserResponseProto.newBuilder();
            for (String group : groups) {
                responseBuilder.addGroups(group);
            }
            return responseBuilder.build();
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public UpdateNodeResourceResponseProto updateNodeResource(RpcController controller,
                                                              UpdateNodeResourceRequestProto proto) throws ServiceException {
        UpdateNodeResourceRequestPBImpl request =
                new UpdateNodeResourceRequestPBImpl(proto);
        try {
            UpdateNodeResourceResponse response = real.updateNodeResource(request);
            return ((UpdateNodeResourceResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public AddToClusterNodeLabelsResponseProto addToClusterNodeLabels(
            RpcController controller, AddToClusterNodeLabelsRequestProto proto)
            throws ServiceException {
        AddToClusterNodeLabelsRequestPBImpl request =
                new AddToClusterNodeLabelsRequestPBImpl(proto);
        try {
            AddToClusterNodeLabelsResponse response =
                    real.addToClusterNodeLabels(request);
            return ((AddToClusterNodeLabelsResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public RemoveFromClusterNodeLabelsResponseProto removeFromClusterNodeLabels(
            RpcController controller, RemoveFromClusterNodeLabelsRequestProto proto)
            throws ServiceException {
        RemoveFromClusterNodeLabelsRequestPBImpl request =
                new RemoveFromClusterNodeLabelsRequestPBImpl(proto);
        try {
            RemoveFromClusterNodeLabelsResponse response =
                    real.removeFromClusterNodeLabels(request);
            return ((RemoveFromClusterNodeLabelsResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public ReplaceLabelsOnNodeResponseProto replaceLabelsOnNodes(
            RpcController controller, ReplaceLabelsOnNodeRequestProto proto)
            throws ServiceException {
        ReplaceLabelsOnNodeRequestPBImpl request =
                new ReplaceLabelsOnNodeRequestPBImpl(proto);
        try {
            ReplaceLabelsOnNodeResponse response = real.replaceLabelsOnNode(request);
            return ((ReplaceLabelsOnNodeResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }
}
