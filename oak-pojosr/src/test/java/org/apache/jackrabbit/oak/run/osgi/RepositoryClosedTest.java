/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.jackrabbit.oak.run.osgi;

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import org.apache.felix.connect.launch.PojoServiceRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepositoryClosedTest extends AbstractRepositoryFactoryTest {

    private static final Logger log = LoggerFactory.getLogger(RepositoryClosedTest.class);

    @Before
    public void setupRepo() {
        config.put(REPOSITORY_CONFIG_FILE, createConfigValue("oak-base-config.json", "oak-tar-config.json"));
    }

    @Test
    public void sessionUsePostClose() throws Exception {
        repository = repositoryFactory.getRepository(config);
        PojoServiceRegistry registry = getRegistry();

        // 1. Obtain handle to a session
        Session s = createAdminSession();

        // 2. Trigger repository shutdown
        disableComponent("org.apache.jackrabbit.oak.jcr.osgi.RepositoryManager");
        disableComponent("org.apache.jackrabbit.oak.plugins.name.NameValidatorProvider");

        log.info("Repository shutdown complete. Proceeding with save");

        // Null out repository to prevent shutdown attempt in teardown
        repository = null;

        // 3. Now try adding a node with invalid name. Such a commit
        // should have got failed
        s.getRootNode().addNode("a\nb");
        try {
            s.save();
            Assert.fail("Session save should have failed due to invalid name");
        } catch (RepositoryException ignore) {
            // Expected exception
        }

        OakOSGiRepositoryFactory.shutdown(registry, 5);
    }
}
