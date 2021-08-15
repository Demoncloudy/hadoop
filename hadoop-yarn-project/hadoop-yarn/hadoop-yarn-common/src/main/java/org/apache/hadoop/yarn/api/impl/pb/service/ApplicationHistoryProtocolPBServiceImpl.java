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
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.*;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.*;

import java.io.IOException;

@Private
public class ApplicationHistoryProtocolPBServiceImpl implements
        ApplicationHistoryProtocolPB {
    private ApplicationHistoryProtocol real;

    public ApplicationHistoryProtocolPBServiceImpl(ApplicationHistoryProtocol impl) {
        this.real = impl;
    }

    @Override
    public GetApplicationReportResponseProto getApplicationReport(
            RpcController arg0, GetApplicationReportRequestProto proto)
            throws ServiceException {
        GetApplicationReportRequestPBImpl request =
                new GetApplicationReportRequestPBImpl(proto);
        try {
            GetApplicationReportResponse response =
                    real.getApplicationReport(request);
            return ((GetApplicationReportResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    @Override
    public GetApplicationsResponseProto getApplications(RpcController controller,
                                                        GetApplicationsRequestProto proto) throws ServiceException {
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
            RenewDelegationTokenResponse response =
                    real.renewDelegationToken(request);
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
            CancelDelegationTokenResponse response =
                    real.cancelDelegationToken(request);
            return ((CancelDelegationTokenResponsePBImpl) response).getProto();
        } catch (YarnException e) {
            throw new ServiceException(e);
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }
}
