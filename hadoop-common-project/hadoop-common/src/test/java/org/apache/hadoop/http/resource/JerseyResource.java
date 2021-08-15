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
package org.apache.hadoop.http.resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.util.ajax.JSON;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * A simple Jersey resource class TestHttpServer.
 * The servlet simply puts the path and the op parameter in a map
 * and return it in JSON format in the response.
 */
@Path("")
public class JerseyResource {
    public static final String PATH = "path";
    public static final String OP = "op";
    static final Log LOG = LogFactory.getLog(JerseyResource.class);

    @GET
    @Path("{" + PATH + ":.*}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response get(
            @PathParam(PATH) @DefaultValue("UNKNOWN_" + PATH) final String path,
            @QueryParam(OP) @DefaultValue("UNKNOWN_" + OP) final String op
    ) throws IOException {
        LOG.info("get: " + PATH + "=" + path + ", " + OP + "=" + op);

        final Map<String, Object> m = new TreeMap<String, Object>();
        m.put(PATH, path);
        m.put(OP, op);
        final String js = JSON.toString(m);
        return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    }
}
