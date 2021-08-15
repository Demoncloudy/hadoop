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
package org.apache.hadoop.ipc;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Return a response in the handler method for the user to see.
 * Useful since you may want to display status to a user even though an
 * error has not occurred.
 */
@InterfaceStability.Unstable
public class RefreshResponse {
    private int returnCode = -1;
    private String message;
    private String senderName;

    // Most RefreshHandlers will use this
    public RefreshResponse(int returnCode, String message) {
        this.returnCode = returnCode;
        this.message = message;
    }

    /**
     * Convenience method to create a response for successful refreshes.
     *
     * @return void response
     */
    public static RefreshResponse successResponse() {
        return new RefreshResponse(0, "Success");
    }

    public String getSenderName() {
        return senderName;
    }

    /**
     * Optionally set the sender of this RefreshResponse.
     * This helps clarify things when multiple handlers respond.
     *
     * @param name The name of the sender
     */
    public void setSenderName(String name) {
        senderName = name;
    }

    public int getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(int rc) {
        returnCode = rc;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String m) {
        message = m;
    }

    @Override
    public String toString() {
        String ret = "";

        if (senderName != null) {
            ret += senderName + ": ";
        }

        if (message != null) {
            ret += message;
        }

        ret += " (exit " + returnCode + ")";
        return ret;
    }
}
