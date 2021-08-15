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

package org.apache.hadoop.mapreduce.v2.api.impl.pb.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.*;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.*;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.*;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;

public class MRClientProtocolPBClientImpl implements MRClientProtocol,
        Closeable {

    protected MRClientProtocolPB proxy;

    public MRClientProtocolPBClientImpl() {
    }

    ;

    public MRClientProtocolPBClientImpl(long clientVersion, InetSocketAddress addr, Configuration conf) throws IOException {
        RPC.setProtocolEngine(conf, MRClientProtocolPB.class, ProtobufRpcEngine.class);
        proxy = RPC.getProxy(MRClientProtocolPB.class, clientVersion, addr, conf);
    }

    @Override
    public InetSocketAddress getConnectAddress() {
        return RPC.getServerAddress(proxy);
    }

    @Override
    public void close() {
        if (this.proxy != null) {
            RPC.stopProxy(this.proxy);
        }
    }

    @Override
    public GetJobReportResponse getJobReport(GetJobReportRequest request)
            throws IOException {
        GetJobReportRequestProto requestProto = ((GetJobReportRequestPBImpl) request).getProto();
        try {
            return new GetJobReportResponsePBImpl(proxy.getJobReport(null, requestProto));
        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    @Override
    public GetTaskReportResponse getTaskReport(GetTaskReportRequest request)
            throws IOException {
        GetTaskReportRequestProto requestProto = ((GetTaskReportRequestPBImpl) request).getProto();
        try {
            return new GetTaskReportResponsePBImpl(proxy.getTaskReport(null, requestProto));
        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    @Override
    public GetTaskAttemptReportResponse getTaskAttemptReport(
            GetTaskAttemptReportRequest request) throws IOException {
        GetTaskAttemptReportRequestProto requestProto = ((GetTaskAttemptReportRequestPBImpl) request).getProto();
        try {
            return new GetTaskAttemptReportResponsePBImpl(proxy.getTaskAttemptReport(null, requestProto));
        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request)
            throws IOException {
        GetCountersRequestProto requestProto = ((GetCountersRequestPBImpl) request).getProto();
        try {
            return new GetCountersResponsePBImpl(proxy.getCounters(null, requestProto));
        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    @Override
    public GetTaskAttemptCompletionEventsResponse getTaskAttemptCompletionEvents(
            GetTaskAttemptCompletionEventsRequest request) throws IOException {
        GetTaskAttemptCompletionEventsRequestProto requestProto = ((GetTaskAttemptCompletionEventsRequestPBImpl) request).getProto();
        try {
            return new GetTaskAttemptCompletionEventsResponsePBImpl(proxy.getTaskAttemptCompletionEvents(null, requestProto));
        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    @Override
    public GetTaskReportsResponse getTaskReports(GetTaskReportsRequest request)
            throws IOException {
        GetTaskReportsRequestProto requestProto = ((GetTaskReportsRequestPBImpl) request).getProto();
        try {
            return new GetTaskReportsResponsePBImpl(proxy.getTaskReports(null, requestProto));
        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    @Override
    public GetDiagnosticsResponse getDiagnostics(GetDiagnosticsRequest request)
            throws IOException {
        GetDiagnosticsRequestProto requestProto = ((GetDiagnosticsRequestPBImpl) request).getProto();
        try {
            return new GetDiagnosticsResponsePBImpl(proxy.getDiagnostics(null, requestProto));
        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    @Override
    public GetDelegationTokenResponse getDelegationToken(
            GetDelegationTokenRequest request) throws IOException {
        GetDelegationTokenRequestProto requestProto = ((GetDelegationTokenRequestPBImpl)
                request).getProto();
        try {
            return new GetDelegationTokenResponsePBImpl(proxy.getDelegationToken(
                    null, requestProto));
        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    @Override
    public KillJobResponse killJob(KillJobRequest request)
            throws IOException {
        KillJobRequestProto requestProto = ((KillJobRequestPBImpl) request).getProto();
        try {
            return new KillJobResponsePBImpl(proxy.killJob(null, requestProto));
        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    @Override
    public KillTaskResponse killTask(KillTaskRequest request)
            throws IOException {
        KillTaskRequestProto requestProto = ((KillTaskRequestPBImpl) request).getProto();
        try {
            return new KillTaskResponsePBImpl(proxy.killTask(null, requestProto));
        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    @Override
    public KillTaskAttemptResponse killTaskAttempt(KillTaskAttemptRequest request)
            throws IOException {
        KillTaskAttemptRequestProto requestProto = ((KillTaskAttemptRequestPBImpl) request).getProto();
        try {
            return new KillTaskAttemptResponsePBImpl(proxy.killTaskAttempt(null, requestProto));
        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    @Override
    public FailTaskAttemptResponse failTaskAttempt(FailTaskAttemptRequest request)
            throws IOException {
        FailTaskAttemptRequestProto requestProto = ((FailTaskAttemptRequestPBImpl) request).getProto();
        try {
            return new FailTaskAttemptResponsePBImpl(proxy.failTaskAttempt(null, requestProto));
        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    @Override
    public RenewDelegationTokenResponse renewDelegationToken(
            RenewDelegationTokenRequest request) throws IOException {
        RenewDelegationTokenRequestProto requestProto =
                ((RenewDelegationTokenRequestPBImpl) request).getProto();
        try {
            return new RenewDelegationTokenResponsePBImpl(proxy.renewDelegationToken(
                    null, requestProto));
        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    @Override
    public CancelDelegationTokenResponse cancelDelegationToken(
            CancelDelegationTokenRequest request) throws IOException {
        CancelDelegationTokenRequestProto requestProto =
                ((CancelDelegationTokenRequestPBImpl) request).getProto();
        try {
            return new CancelDelegationTokenResponsePBImpl(
                    proxy.cancelDelegationToken(null, requestProto));

        } catch (ServiceException e) {
            throw unwrapAndThrowException(e);
        }
    }

    private IOException unwrapAndThrowException(ServiceException se) {
        if (se.getCause() instanceof RemoteException) {
            return ((RemoteException) se.getCause()).unwrapRemoteException();
        } else if (se.getCause() instanceof IOException) {
            return (IOException) se.getCause();
        } else {
            throw new UndeclaredThrowableException(se.getCause());
        }
    }
}
