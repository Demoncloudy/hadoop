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
package org.apache.hadoop.nfs.nfs3.request;

import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.oncrpc.XDR;

import java.io.IOException;

/**
 * MKDIR3 Request
 */
public class MKDIR3Request extends RequestWithHandle {
    private final String name;
    private final SetAttr3 objAttr;

    public MKDIR3Request(FileHandle handle, String name, SetAttr3 objAttr) {
        super(handle);
        this.name = name;
        this.objAttr = objAttr;
    }

    public static MKDIR3Request deserialize(XDR xdr) throws IOException {
        FileHandle handle = readHandle(xdr);
        String name = xdr.readString();
        SetAttr3 objAttr = new SetAttr3();
        objAttr.deserialize(xdr);
        return new MKDIR3Request(handle, name, objAttr);
    }

    public String getName() {
        return name;
    }

    public SetAttr3 getObjAttr() {
        return objAttr;
    }

    @Override
    public void serialize(XDR xdr) {
        handle.serialize(xdr);
        xdr.writeInt(name.getBytes().length);
        xdr.writeFixedOpaque(name.getBytes());
        objAttr.serialize(xdr);
    }
}