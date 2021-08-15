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

package org.apache.hadoop.yarn.api.impl.pb.service;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.*;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.*;

import java.io.IOException;

@Private
public class ContainerManagementProtocolPBServiceImpl implements ContainerManagementProtocolPB {

    private ContainerManagementProtocol real;

    public ContainerManagementProtocolPBServiceImpl(ContainerManagementProtocol impl) {
        this.real = impl;
    }

    @Override
    public StartContainersResponseProto startContainers(RpcController arg0,
                                                        StartContainersRequestProto proto) throws ServiceException {
        StartContainersRequestPBImpl request = new StartContainersRequestPBImpl(proto);
        try {
            StartContainersResponse response = real.startContainers(request);
            return ((StartContainersResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public StopContainersResponseProto stopContainers(RpcController arg0,
                                                      StopContainersRequestProto proto) throws ServiceException {
        StopContainersRequestPBImpl request = new StopContainersRequestPBImpl(proto);
        try {
            StopContainersResponse response = real.stopContainers(request);
            return ((StopContainersResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetContainerStatusesResponseProto getContainerStatuses(
            RpcController arg0, GetContainerStatusesRequestProto proto)
            throws ServiceException {
        GetContainerStatusesRequestPBImpl request = new GetContainerStatusesRequestPBImpl(proto);
        try {
            GetContainerStatusesResponse response = real.getContainerStatuses(request);
            return ((GetContainerStatusesResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }
}
