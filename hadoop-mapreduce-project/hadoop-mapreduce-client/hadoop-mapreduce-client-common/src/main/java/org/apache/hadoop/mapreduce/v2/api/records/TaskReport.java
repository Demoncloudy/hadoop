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

package org.apache.hadoop.mapreduce.v2.api.records;

import java.util.List;

public interface TaskReport {
    public abstract TaskId getTaskId();

    public abstract void setTaskId(TaskId taskId);

    public abstract TaskState getTaskState();

    public abstract void setTaskState(TaskState taskState);

    public abstract float getProgress();

    public abstract void setProgress(float progress);

    public abstract String getStatus();

    public abstract void setStatus(String status);

    public abstract long getStartTime();

    public abstract void setStartTime(long startTime);

    public abstract long getFinishTime();

    public abstract void setFinishTime(long finishTime);

    public abstract Counters getCounters();

    public abstract void setCounters(Counters counters);

    public abstract List<TaskAttemptId> getRunningAttemptsList();

    public abstract TaskAttemptId getRunningAttempt(int index);

    public abstract int getRunningAttemptsCount();

    public abstract TaskAttemptId getSuccessfulAttempt();

    public abstract void setSuccessfulAttempt(TaskAttemptId taskAttempt)
            ;

    public abstract List<String> getDiagnosticsList();

    public abstract String getDiagnostics(int index);

    public abstract int getDiagnosticsCount();

    public abstract void addAllRunningAttempts(List<TaskAttemptId> taskAttempts);

    public abstract void addRunningAttempt(TaskAttemptId taskAttempt);

    public abstract void removeRunningAttempt(int index);

    public abstract void clearRunningAttempts();

    public abstract void addAllDiagnostics(List<String> diagnostics);

    public abstract void addDiagnostics(String diagnostics);

    public abstract void removeDiagnostics(int index);

    public abstract void clearDiagnostics();
}
