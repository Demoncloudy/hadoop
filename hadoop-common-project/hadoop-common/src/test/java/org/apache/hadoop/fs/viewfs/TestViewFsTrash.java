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
package org.apache.hadoop.fs.viewfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestViewFsTrash {
    FileSystem fsTarget;  // the target file system - the mount will point here
    FileSystem fsView;
    Configuration conf;
    FileSystemTestHelper fileSystemTestHelper = new FileSystemTestHelper();

    @Before
    public void setUp() throws Exception {
        fsTarget = FileSystem.getLocal(new Configuration());
        fsTarget.mkdirs(new Path(fileSystemTestHelper.
                getTestRootPath(fsTarget), "dir1"));
        conf = ViewFileSystemTestSetup.createConfig();
        fsView = ViewFileSystemTestSetup.setupForViewFileSystem(conf, fileSystemTestHelper, fsTarget);
        conf.set("fs.defaultFS", FsConstants.VIEWFS_URI.toString());
    }

    @After
    public void tearDown() throws Exception {
        ViewFileSystemTestSetup.tearDown(fileSystemTestHelper, fsTarget);
        fsTarget.delete(new Path(fsTarget.getHomeDirectory(), ".Trash/Current"),
                true);
    }

    @Test
    public void testTrash() throws IOException {
        TestTrash.trashShell(conf, fileSystemTestHelper.getTestRootPath(fsView),
                fsTarget, new Path(fsTarget.getHomeDirectory(), ".Trash/Current"));
    }

    class TestLFS extends LocalFileSystem {
        Path home;

        TestLFS() throws IOException {
            this(new Path(fileSystemTestHelper.getTestRootDir()));
        }

        TestLFS(Path home) throws IOException {
            super();
            this.home = home;
        }

        @Override
        public Path getHomeDirectory() {
            return home;
        }
    }

}
