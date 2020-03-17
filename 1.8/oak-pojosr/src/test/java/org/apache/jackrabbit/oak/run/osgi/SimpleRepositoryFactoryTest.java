/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.run.osgi;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jcr.Repository;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.commons.JcrUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertNotNull;

public class SimpleRepositoryFactoryTest {
    @Rule
    public final TemporaryFolder tmpFolder = new TemporaryFolder(new File("target"));

    @Test(timeout = 60 * 1000)
    public void testRepositoryService() throws Exception{
        Map<String,String> config = new HashMap<String, String>();
        config.put("org.apache.jackrabbit.repository.home",
                tmpFolder.getRoot().getAbsolutePath());
        config.put("org.apache.jackrabbit.oak.repository.configFile",
                path("oak-base-config.json")+","+path("oak-tar-config.json"));

        Repository repository = JcrUtils.getRepository(config);
        assertNotNull(repository);
        ((JackrabbitRepository)repository).shutdown();
    }



    private static String getBaseDir() {
        // 'basedir' is set by Maven Surefire. It always points to the current subproject,
        // even in reactor builds.
        String baseDir = System.getProperty("basedir");
        if(baseDir != null) {
            return baseDir;
        }
        return new File(".").getAbsolutePath();
    }

    private static String path(String path){
        File file = new File(FilenameUtils.concat(getBaseDir(), "src/test/resources/"+path));
        assert file.exists() : "No file found at " + file.getAbsolutePath();
        return file.getAbsolutePath();
    }
}
