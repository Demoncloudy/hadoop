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
package org.apache.hadoop.security.authorize;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestProxyServers {

    @Test
    public void testProxyServer() {
        Configuration conf = new Configuration();
        assertFalse(ProxyServers.isProxyServer("1.1.1.1"));
        conf.set(ProxyServers.CONF_HADOOP_PROXYSERVERS, "2.2.2.2, 3.3.3.3");
        ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
        assertFalse(ProxyServers.isProxyServer("1.1.1.1"));
        assertTrue(ProxyServers.isProxyServer("2.2.2.2"));
        assertTrue(ProxyServers.isProxyServer("3.3.3.3"));
    }
}
