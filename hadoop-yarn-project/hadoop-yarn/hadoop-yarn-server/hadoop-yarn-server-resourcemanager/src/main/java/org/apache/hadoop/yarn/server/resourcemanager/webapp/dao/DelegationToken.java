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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "delegation-token")
@XmlAccessorType(XmlAccessType.FIELD)
public class DelegationToken {

    String token;
    String renewer;
    String owner;
    String kind;
    @XmlElement(name = "expiration-time")
    Long nextExpirationTime;
    @XmlElement(name = "max-validity")
    Long maxValidity;

    public DelegationToken() {
    }

    public DelegationToken(String token, String renewer, String owner,
                           String kind, Long nextExpirationTime, Long maxValidity) {
        this.token = token;
        this.renewer = renewer;
        this.owner = owner;
        this.kind = kind;
        this.nextExpirationTime = nextExpirationTime;
        this.maxValidity = maxValidity;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getRenewer() {
        return renewer;
    }

    public void setRenewer(String renewer) {
        this.renewer = renewer;
    }

    public Long getNextExpirationTime() {
        return nextExpirationTime;
    }

    public void setNextExpirationTime(long nextExpirationTime) {
        this.nextExpirationTime = Long.valueOf(nextExpirationTime);
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public Long getMaxValidity() {
        return maxValidity;
    }

    public void setMaxValidity(Long maxValidity) {
        this.maxValidity = maxValidity;
    }
}
