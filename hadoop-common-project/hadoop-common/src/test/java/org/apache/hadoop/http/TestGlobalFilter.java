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
package org.apache.hadoop.http;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Set;
import java.util.TreeSet;

public class TestGlobalFilter extends HttpServerFunctionalTest {
    static final Log LOG = LogFactory.getLog(HttpServer2.class);
    static final Set<String> RECORDS = new TreeSet<String>();

    /**
     * access a url, ignoring some IOException such as the page does not exist
     */
    static void access(String urlstring) throws IOException {
        LOG.warn("access " + urlstring);
        URL url = new URL(urlstring);
        URLConnection connection = url.openConnection();
        connection.connect();

        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            try {
                for (; in.readLine() != null; ) ;
            } finally {
                in.close();
            }
        } catch (IOException ioe) {
            LOG.warn("urlstring=" + urlstring, ioe);
        }
    }

    @Test
    public void testServletFilter() throws Exception {
        Configuration conf = new Configuration();

        //start a http server with CountingFilter
        conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
                RecordingFilter.Initializer.class.getName());
        HttpServer2 http = createTestServer(conf);
        http.start();

        final String fsckURL = "/fsck";
        final String stacksURL = "/stacks";
        final String ajspURL = "/a.jsp";
        final String listPathsURL = "/listPaths";
        final String dataURL = "/data";
        final String streamFile = "/streamFile";
        final String rootURL = "/";
        final String allURL = "/*";
        final String outURL = "/static/a.out";
        final String logURL = "/logs/a.log";

        final String[] urls = {fsckURL, stacksURL, ajspURL, listPathsURL,
                dataURL, streamFile, rootURL, allURL, outURL, logURL};

        //access the urls
        final String prefix = "http://"
                + NetUtils.getHostPortString(http.getConnectorAddress(0));
        try {
            for (int i = 0; i < urls.length; i++) {
                access(prefix + urls[i]);
            }
        } finally {
            http.stop();
        }

        LOG.info("RECORDS = " + RECORDS);

        //verify records
        for (int i = 0; i < urls.length; i++) {
            assertTrue(RECORDS.remove(urls[i]));
        }
        assertTrue(RECORDS.isEmpty());
    }

    /**
     * A very simple filter that records accessed uri's
     */
    static public class RecordingFilter implements Filter {
        private FilterConfig filterConfig = null;

        @Override
        public void init(FilterConfig filterConfig) {
            this.filterConfig = filterConfig;
        }

        @Override
        public void destroy() {
            this.filterConfig = null;
        }

        @Override
        public void doFilter(ServletRequest request, ServletResponse response,
                             FilterChain chain) throws IOException, ServletException {
            if (filterConfig == null)
                return;

            String uri = ((HttpServletRequest) request).getRequestURI();
            LOG.info("filtering " + uri);
            RECORDS.add(uri);
            chain.doFilter(request, response);
        }

        /**
         * Configuration for RecordingFilter
         */
        static public class Initializer extends FilterInitializer {
            public Initializer() {
            }

            @Override
            public void initFilter(FilterContainer container, Configuration conf) {
                container.addGlobalFilter("recording", RecordingFilter.class.getName(), null);
            }
        }
    }
}
