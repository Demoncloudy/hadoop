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

package org.apache.hadoop.yarn.api.records.impl.pb;

import com.google.protobuf.ByteString;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.proto.YarnProtos.*;
import org.apache.hadoop.yarn.proto.YarnServiceProtos;

import java.nio.ByteBuffer;

@Private
@Unstable
public class ProtoUtils {


    /*
     * ContainerState
     */
    private static String CONTAINER_STATE_PREFIX = "C_";
    /*
     * NodeState
     */
    private static String NODE_STATE_PREFIX = "NS_";
    /*
     * YarnApplicationAttemptState
     */
    private static String YARN_APPLICATION_ATTEMPT_STATE_PREFIX = "APP_ATTEMPT_";
    /*
     * FinalApplicationStatus
     */
    private static String FINAL_APPLICATION_STATUS_PREFIX = "APP_";
    /*
     * QueueState
     */
    private static String QUEUE_STATE_PREFIX = "Q_";
    /*
     * QueueACL
     */
    private static String QUEUE_ACL_PREFIX = "QACL_";
    /*
     * ApplicationAccessType
     */
    private static String APP_ACCESS_TYPE_PREFIX = "APPACCESS_";

    public static ContainerStateProto convertToProtoFormat(ContainerState e) {
        return ContainerStateProto.valueOf(CONTAINER_STATE_PREFIX + e.name());
    }

    public static ContainerState convertFromProtoFormat(ContainerStateProto e) {
        return ContainerState.valueOf(e.name().replace(CONTAINER_STATE_PREFIX, ""));
    }

    public static NodeStateProto convertToProtoFormat(NodeState e) {
        return NodeStateProto.valueOf(NODE_STATE_PREFIX + e.name());
    }

    public static NodeState convertFromProtoFormat(NodeStateProto e) {
        return NodeState.valueOf(e.name().replace(NODE_STATE_PREFIX, ""));
    }

    /*
     * NodeId
     */
    public static NodeIdProto convertToProtoFormat(NodeId e) {
        return ((NodeIdPBImpl) e).getProto();
    }

    public static NodeId convertFromProtoFormat(NodeIdProto e) {
        return new NodeIdPBImpl(e);
    }

    /*
     * YarnApplicationState
     */
    public static YarnApplicationStateProto convertToProtoFormat(YarnApplicationState e) {
        return YarnApplicationStateProto.valueOf(e.name());
    }

    public static YarnApplicationState convertFromProtoFormat(YarnApplicationStateProto e) {
        return YarnApplicationState.valueOf(e.name());
    }

    public static YarnApplicationAttemptStateProto convertToProtoFormat(
            YarnApplicationAttemptState e) {
        return YarnApplicationAttemptStateProto
                .valueOf(YARN_APPLICATION_ATTEMPT_STATE_PREFIX + e.name());
    }

    public static YarnApplicationAttemptState convertFromProtoFormat(
            YarnApplicationAttemptStateProto e) {
        return YarnApplicationAttemptState.valueOf(e.name().replace(
                YARN_APPLICATION_ATTEMPT_STATE_PREFIX, ""));
    }

    /*
     * ApplicationsRequestScope
     */
    public static YarnServiceProtos.ApplicationsRequestScopeProto
    convertToProtoFormat(ApplicationsRequestScope e) {
        return YarnServiceProtos.ApplicationsRequestScopeProto.valueOf(e.name());
    }

    public static ApplicationsRequestScope convertFromProtoFormat
            (YarnServiceProtos.ApplicationsRequestScopeProto e) {
        return ApplicationsRequestScope.valueOf(e.name());
    }

    /*
     * ApplicationResourceUsageReport
     */
    public static ApplicationResourceUsageReportProto convertToProtoFormat(ApplicationResourceUsageReport e) {
        return ((ApplicationResourceUsageReportPBImpl) e).getProto();
    }

    public static ApplicationResourceUsageReport convertFromProtoFormat(ApplicationResourceUsageReportProto e) {
        return new ApplicationResourceUsageReportPBImpl(e);
    }

    public static FinalApplicationStatusProto convertToProtoFormat(FinalApplicationStatus e) {
        return FinalApplicationStatusProto.valueOf(FINAL_APPLICATION_STATUS_PREFIX + e.name());
    }

    public static FinalApplicationStatus convertFromProtoFormat(FinalApplicationStatusProto e) {
        return FinalApplicationStatus.valueOf(e.name().replace(FINAL_APPLICATION_STATUS_PREFIX, ""));
    }

    /*
     * LocalResourceType
     */
    public static LocalResourceTypeProto convertToProtoFormat(LocalResourceType e) {
        return LocalResourceTypeProto.valueOf(e.name());
    }

    public static LocalResourceType convertFromProtoFormat(LocalResourceTypeProto e) {
        return LocalResourceType.valueOf(e.name());
    }

    /*
     * LocalResourceVisibility
     */
    public static LocalResourceVisibilityProto convertToProtoFormat(LocalResourceVisibility e) {
        return LocalResourceVisibilityProto.valueOf(e.name());
    }

    public static LocalResourceVisibility convertFromProtoFormat(LocalResourceVisibilityProto e) {
        return LocalResourceVisibility.valueOf(e.name());
    }

    /*
     * AMCommand
     */
    public static AMCommandProto convertToProtoFormat(AMCommand e) {
        return AMCommandProto.valueOf(e.name());
    }

    public static AMCommand convertFromProtoFormat(AMCommandProto e) {
        return AMCommand.valueOf(e.name());
    }

    /*
     * ByteBuffer
     */
    public static ByteBuffer convertFromProtoFormat(ByteString byteString) {
        int capacity = byteString.asReadOnlyByteBuffer().rewind().remaining();
        byte[] b = new byte[capacity];
        byteString.asReadOnlyByteBuffer().get(b, 0, capacity);
        return ByteBuffer.wrap(b);
    }

    public static ByteString convertToProtoFormat(ByteBuffer byteBuffer) {
//    return ByteString.copyFrom((ByteBuffer)byteBuffer.duplicate().rewind());
        int oldPos = byteBuffer.position();
        byteBuffer.rewind();
        ByteString bs = ByteString.copyFrom(byteBuffer);
        byteBuffer.position(oldPos);
        return bs;
    }

    public static QueueStateProto convertToProtoFormat(QueueState e) {
        return QueueStateProto.valueOf(QUEUE_STATE_PREFIX + e.name());
    }

    public static QueueState convertFromProtoFormat(QueueStateProto e) {
        return QueueState.valueOf(e.name().replace(QUEUE_STATE_PREFIX, ""));
    }

    public static QueueACLProto convertToProtoFormat(QueueACL e) {
        return QueueACLProto.valueOf(QUEUE_ACL_PREFIX + e.name());
    }

    public static QueueACL convertFromProtoFormat(QueueACLProto e) {
        return QueueACL.valueOf(e.name().replace(QUEUE_ACL_PREFIX, ""));
    }

    public static ApplicationAccessTypeProto convertToProtoFormat(
            ApplicationAccessType e) {
        return ApplicationAccessTypeProto.valueOf(APP_ACCESS_TYPE_PREFIX
                + e.name());
    }

    public static ApplicationAccessType convertFromProtoFormat(
            ApplicationAccessTypeProto e) {
        return ApplicationAccessType.valueOf(e.name().replace(
                APP_ACCESS_TYPE_PREFIX, ""));
    }

    /*
     * Reservation Request interpreter type
     */
    public static ReservationRequestInterpreterProto convertToProtoFormat(
            ReservationRequestInterpreter e) {
        return ReservationRequestInterpreterProto.valueOf(e.name());
    }

    public static ReservationRequestInterpreter convertFromProtoFormat(
            ReservationRequestInterpreterProto e) {
        return ReservationRequestInterpreter.valueOf(e.name());
    }

}
