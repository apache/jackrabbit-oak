/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak;

import javax.jcr.NoSuchWorkspaceException;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * OakTest... TODO
 */
public class OakTest {

    @Test
    public void testWithDefaultWorkspaceName() throws Exception {
        ContentRepository repo = new Oak().with("test").with(new OpenSecurityProvider()).createContentRepository();

        String[] valid = new String[] {null, "test"};
        for (String wspName : valid) {
            ContentSession cs = null;
            try {
                cs = repo.login(null, wspName);
                assertEquals("test", cs.getWorkspaceName());
            } finally {
                if (cs != null) {
                    cs.close();
                }
            }
        }

        String[] invalid = new String[] {"", "another", Oak.DEFAULT_WORKSPACE_NAME};
        for (String wspName : invalid) {
            ContentSession cs = null;
            try {
                cs = repo.login(null, wspName);
                fail("invalid workspace nam");
            } catch (NoSuchWorkspaceException e) {
                // success
            } finally {
                if (cs != null) {
                    cs.close();
                }
            }
        }

    }

}