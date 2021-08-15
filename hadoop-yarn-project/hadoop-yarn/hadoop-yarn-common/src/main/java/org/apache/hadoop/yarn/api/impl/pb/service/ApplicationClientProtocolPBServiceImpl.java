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
import org.apache.hadoop.security.proto.SecurityProtos.*;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationClientProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.*;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.*;

import java.io.IOException;

@Private
public class ApplicationClientProtocolPBServiceImpl implements ApplicationClientProtocolPB {

    private ApplicationClientProtocol real;

    public ApplicationClientProtocolPBServiceImpl(ApplicationClientProtocol impl) {
        this.real = impl;
    }

    @Override
    public KillApplicationResponseProto forceKillApplication(RpcController arg0,
                                                             KillApplicationRequestProto proto) throws ServiceException {
        KillApplicationRequestPBImpl request = new KillApplicationRequestPBImpl(proto);
        try {
            KillApplicationResponse response = real.forceKillApplication(request);
            return ((KillApplicationResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetApplicationReportResponseProto getApplicationReport(
            RpcController arg0, GetApplicationReportRequestProto proto)
            throws ServiceException {
        GetApplicationReportRequestPBImpl request = new GetApplicationReportRequestPBImpl(proto);
        try {
            GetApplicationReportResponse response = real.getApplicationReport(request);
            return ((GetApplicationReportResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetClusterMetricsResponseProto getClusterMetrics(RpcController arg0,
                                                            GetClusterMetricsRequestProto proto) throws ServiceException {
        GetClusterMetricsRequestPBImpl request = new GetClusterMetricsRequestPBImpl(proto);
        try {
            GetClusterMetricsResponse response = real.getClusterMetrics(request);
            return ((GetClusterMetricsResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetNewApplicationResponseProto getNewApplication(
            RpcController arg0, GetNewApplicationRequestProto proto)
            throws ServiceException {
        GetNewApplicationRequestPBImpl request = new GetNewApplicationRequestPBImpl(proto);
        try {
            GetNewApplicationResponse response = real.getNewApplication(request);
            return ((GetNewApplicationResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public SubmitApplicationResponseProto submitApplication(RpcController arg0,
                                                            SubmitApplicationRequestProto proto) throws ServiceException {
        SubmitApplicationRequestPBImpl request = new SubmitApplicationRequestPBImpl(proto);
        try {
            SubmitApplicationResponse response = real.submitApplication(request);
            return ((SubmitApplicationResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetApplicationsResponseProto getApplications(
            RpcController controller, GetApplicationsRequestProto proto)
            throws ServiceException {
        GetApplicationsRequestPBImpl request =
                new GetApplicationsRequestPBImpl(proto);
        try {
            GetApplicationsResponse response = real.getApplications(request);
            return ((GetApplicationsResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetClusterNodesResponseProto getClusterNodes(RpcController controller,
                                                        GetClusterNodesRequestProto proto) throws ServiceException {
        GetClusterNodesRequestPBImpl request =
                new GetClusterNodesRequestPBImpl(proto);
        try {
            GetClusterNodesResponse response = real.getClusterNodes(request);
            return ((GetClusterNodesResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetQueueInfoResponseProto getQueueInfo(RpcController controller,
                                                  GetQueueInfoRequestProto proto) throws ServiceException {
        GetQueueInfoRequestPBImpl request =
                new GetQueueInfoRequestPBImpl(proto);
        try {
            GetQueueInfoResponse response = real.getQueueInfo(request);
            return ((GetQueueInfoResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetQueueUserAclsInfoResponseProto getQueueUserAcls(
            RpcController controller, GetQueueUserAclsInfoRequestProto proto)
            throws ServiceException {
        GetQueueUserAclsInfoRequestPBImpl request =
                new GetQueueUserAclsInfoRequestPBImpl(proto);
        try {
            GetQueueUserAclsInfoResponse response = real.getQueueUserAcls(request);
            return ((GetQueueUserAclsInfoResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetDelegationTokenResponseProto getDelegationToken(
            RpcController controller, GetDelegationTokenRequestProto proto)
            throws ServiceException {
        GetDelegationTokenRequestPBImpl request =
                new GetDelegationTokenRequestPBImpl(proto);
        try {
            GetDelegationTokenResponse response = real.getDelegationToken(request);
            return ((GetDelegationTokenResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public RenewDelegationTokenResponseProto renewDelegationToken(
            RpcController controller, RenewDelegationTokenRequestProto proto)
            throws ServiceException {
        RenewDelegationTokenRequestPBImpl request =
                new RenewDelegationTokenRequestPBImpl(proto);
        try {
            RenewDelegationTokenResponse response = real.renewDelegationToken(request);
            return ((RenewDelegationTokenResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public CancelDelegationTokenResponseProto cancelDelegationToken(
            RpcController controller, CancelDelegationTokenRequestProto proto)
            throws ServiceException {
        CancelDelegationTokenRequestPBImpl request =
                new CancelDelegationTokenRequestPBImpl(proto);
        try {
            CancelDelegationTokenResponse response = real.cancelDelegationToken(request);
            return ((CancelDelegationTokenResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public MoveApplicationAcrossQueuesResponseProto moveApplicationAcrossQueues(
            RpcController controller, MoveApplicationAcrossQueuesRequestProto proto)
            throws ServiceException {
        MoveApplicationAcrossQueuesRequestPBImpl request =
                new MoveApplicationAcrossQueuesRequestPBImpl(proto);
        try {
            MoveApplicationAcrossQueuesResponse response = real.moveApplicationAcrossQueues(request);
            return ((MoveApplicationAcrossQueuesResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetApplicationAttemptReportResponseProto getApplicationAttemptReport(
            RpcController controller, GetApplicationAttemptReportRequestProto proto)
            throws ServiceException {
        GetApplicationAttemptReportRequestPBImpl request =
                new GetApplicationAttemptReportRequestPBImpl(proto);
        try {
            GetApplicationAttemptReportResponse response =
                    real.getApplicationAttemptReport(request);
            return ((GetApplicationAttemptReportResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetApplicationAttemptsResponseProto getApplicationAttempts(
            RpcController controller, GetApplicationAttemptsRequestProto proto)
            throws ServiceException {
        GetApplicationAttemptsRequestPBImpl request =
                new GetApplicationAttemptsRequestPBImpl(proto);
        try {
            GetApplicationAttemptsResponse response =
                    real.getApplicationAttempts(request);
            return ((GetApplicationAttemptsResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetContainerReportResponseProto getContainerReport(
            RpcController controller, GetContainerReportRequestProto proto)
            throws ServiceException {
        GetContainerReportRequestPBImpl request =
                new GetContainerReportRequestPBImpl(proto);
        try {
            GetContainerReportResponse response = real.getContainerReport(request);
            return ((GetContainerReportResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetContainersResponseProto getContainers(RpcController controller,
                                                    GetContainersRequestProto proto) throws ServiceException {
        GetContainersRequestPBImpl request = new GetContainersRequestPBImpl(proto);
        try {
            GetContainersResponse response = real.getContainers(request);
            return ((GetContainersResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public ReservationSubmissionResponseProto submitReservation(RpcController controller,
                                                                ReservationSubmissionRequestProto requestProto) throws ServiceException {
        ReservationSubmissionRequestPBImpl request =
                new ReservationSubmissionRequestPBImpl(requestProto);
        try {
            ReservationSubmissionResponse response = real.submitReservation(request);
            return ((ReservationSubmissionResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public ReservationUpdateResponseProto updateReservation(RpcController controller,
                                                            ReservationUpdateRequestProto requestProto) throws ServiceException {
        ReservationUpdateRequestPBImpl request =
                new ReservationUpdateRequestPBImpl(requestProto);
        try {
            ReservationUpdateResponse response = real.updateReservation(request);
            return ((ReservationUpdateResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public ReservationDeleteResponseProto deleteReservation(RpcController controller,
                                                            ReservationDeleteRequestProto requestProto) throws ServiceException {
        ReservationDeleteRequestPBImpl request =
                new ReservationDeleteRequestPBImpl(requestProto);
        try {
            ReservationDeleteResponse response = real.deleteReservation(request);
            return ((ReservationDeleteResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetNodesToLabelsResponseProto getNodeToLabels(
            RpcController controller, GetNodesToLabelsRequestProto proto)
            throws ServiceException {
        GetNodesToLabelsRequestPBImpl request =
                new GetNodesToLabelsRequestPBImpl(proto);
        try {
            GetNodesToLabelsResponse response = real.getNodeToLabels(request);
            return ((GetNodesToLabelsResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetClusterNodeLabelsResponseProto getClusterNodeLabels(
            RpcController controller, GetClusterNodeLabelsRequestProto proto)
            throws ServiceException {
        GetClusterNodeLabelsRequestPBImpl request =
                new GetClusterNodeLabelsRequestPBImpl(proto);
        try {
            GetClusterNodeLabelsResponse response =
                    real.getClusterNodeLabels(request);
            return ((GetClusterNodeLabelsResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }
}
