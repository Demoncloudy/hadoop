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

package org.apache.hadoop.mapred;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.*;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;

public class NotRunningJob implements MRClientProtocol {

    private final JobState jobState;
    private final ApplicationReport applicationReport;
    private RecordFactory recordFactory =
            RecordFactoryProvider.getRecordFactory(null);


    NotRunningJob(ApplicationReport applicationReport, JobState jobState) {
        this.applicationReport =
                (applicationReport == null) ?
                        getUnknownApplicationReport() : applicationReport;
        this.jobState = jobState;
    }

    private ApplicationReport getUnknownApplicationReport() {
        ApplicationId unknownAppId = recordFactory
                .newRecordInstance(ApplicationId.class);
        ApplicationAttemptId unknownAttemptId = recordFactory
                .newRecordInstance(ApplicationAttemptId.class);

        // Setting AppState to NEW and finalStatus to UNDEFINED as they are never
        // used for a non running job
        return ApplicationReport.newInstance(unknownAppId, unknownAttemptId,
                "N/A", "N/A", "N/A", "N/A", 0, null, YarnApplicationState.NEW, "N/A",
                "N/A", 0, 0, FinalApplicationStatus.UNDEFINED, null, "N/A", 0.0f,
                YarnConfiguration.DEFAULT_APPLICATION_TYPE, null);
    }

    @Override
    public FailTaskAttemptResponse failTaskAttempt(
            FailTaskAttemptRequest request) throws IOException {
        FailTaskAttemptResponse resp =
                recordFactory.newRecordInstance(FailTaskAttemptResponse.class);
        return resp;
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request)
            throws IOException {
        GetCountersResponse resp =
                recordFactory.newRecordInstance(GetCountersResponse.class);
        Counters counters = recordFactory.newRecordInstance(Counters.class);
        counters.addAllCounterGroups(new HashMap<String, CounterGroup>());
        resp.setCounters(counters);
        return resp;
    }

    @Override
    public GetDiagnosticsResponse getDiagnostics(GetDiagnosticsRequest request)
            throws IOException {
        GetDiagnosticsResponse resp =
                recordFactory.newRecordInstance(GetDiagnosticsResponse.class);
        resp.addDiagnostics("");
        return resp;
    }

    @Override
    public GetJobReportResponse getJobReport(GetJobReportRequest request)
            throws IOException {
        JobReport jobReport =
                recordFactory.newRecordInstance(JobReport.class);
        jobReport.setJobId(request.getJobId());
        jobReport.setJobState(jobState);
        jobReport.setUser(applicationReport.getUser());
        jobReport.setStartTime(applicationReport.getStartTime());
        jobReport.setDiagnostics(applicationReport.getDiagnostics());
        jobReport.setJobName(applicationReport.getName());
        jobReport.setTrackingUrl(applicationReport.getTrackingUrl());
        jobReport.setFinishTime(applicationReport.getFinishTime());

        GetJobReportResponse resp =
                recordFactory.newRecordInstance(GetJobReportResponse.class);
        resp.setJobReport(jobReport);
        return resp;
    }

    @Override
    public GetTaskAttemptCompletionEventsResponse getTaskAttemptCompletionEvents(
            GetTaskAttemptCompletionEventsRequest request)
            throws IOException {
        GetTaskAttemptCompletionEventsResponse resp =
                recordFactory.newRecordInstance(GetTaskAttemptCompletionEventsResponse.class);
        resp.addAllCompletionEvents(new ArrayList<TaskAttemptCompletionEvent>());
        return resp;
    }

    @Override
    public GetTaskAttemptReportResponse getTaskAttemptReport(
            GetTaskAttemptReportRequest request) throws IOException {
        //not invoked by anybody
        throw new NotImplementedException();
    }

    @Override
    public GetTaskReportResponse getTaskReport(GetTaskReportRequest request)
            throws IOException {
        GetTaskReportResponse resp =
                recordFactory.newRecordInstance(GetTaskReportResponse.class);
        TaskReport report = recordFactory.newRecordInstance(TaskReport.class);
        report.setTaskId(request.getTaskId());
        report.setTaskState(TaskState.NEW);
        Counters counters = recordFactory.newRecordInstance(Counters.class);
        counters.addAllCounterGroups(new HashMap<String, CounterGroup>());
        report.setCounters(counters);
        report.addAllRunningAttempts(new ArrayList<TaskAttemptId>());
        return resp;
    }

    @Override
    public GetTaskReportsResponse getTaskReports(GetTaskReportsRequest request)
            throws IOException {
        GetTaskReportsResponse resp =
                recordFactory.newRecordInstance(GetTaskReportsResponse.class);
        resp.addAllTaskReports(new ArrayList<TaskReport>());
        return resp;
    }

    @Override
    public KillJobResponse killJob(KillJobRequest request)
            throws IOException {
        KillJobResponse resp =
                recordFactory.newRecordInstance(KillJobResponse.class);
        return resp;
    }

    @Override
    public KillTaskResponse killTask(KillTaskRequest request)
            throws IOException {
        KillTaskResponse resp =
                recordFactory.newRecordInstance(KillTaskResponse.class);
        return resp;
    }

    @Override
    public KillTaskAttemptResponse killTaskAttempt(
            KillTaskAttemptRequest request) throws IOException {
        KillTaskAttemptResponse resp =
                recordFactory.newRecordInstance(KillTaskAttemptResponse.class);
        return resp;
    }

    @Override
    public GetDelegationTokenResponse getDelegationToken(
            GetDelegationTokenRequest request) throws IOException {
        /* Should not be invoked by anyone. */
        throw new NotImplementedException();
    }

    @Override
    public RenewDelegationTokenResponse renewDelegationToken(
            RenewDelegationTokenRequest request) throws IOException {
        /* Should not be invoked by anyone. */
        throw new NotImplementedException();
    }

    @Override
    public CancelDelegationTokenResponse cancelDelegationToken(
            CancelDelegationTokenRequest request) throws IOException {
        /* Should not be invoked by anyone. */
        throw new NotImplementedException();
    }

    @Override
    public InetSocketAddress getConnectAddress() {
        /* Should not be invoked by anyone.  Normally used to set token service */
        throw new NotImplementedException();
    }
}
