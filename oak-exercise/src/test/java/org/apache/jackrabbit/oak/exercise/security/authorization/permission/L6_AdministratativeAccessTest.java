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
package org.apache.jackrabbit.oak.exercise.security.authorization.permission;

import javax.jcr.AccessDeniedException;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

/**
 * <pre>
 * Module: Authorization (Permission Evaluation)
 * =============================================================================
 *
 * Title: Administrative Permissions
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Learn how the default implementation handles administrative access and makes
 * sure administrative session always have full access to the repository and
 * cannot be locked out.
 *
 * Exercises:
 *
 * - {@link #testAdmininistrativePermissions()}
 *   Use this test to walk through both read and write access with an administrative
 *   session.
 *
 *   Question: Can you identify where there administrative permissions are being evaluated?
 *   Question: Can you list the differences compared to regular permission evaluation?
 *   Question: Can you explain, where the different handling is started and what are the criteria?
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link #testAdministrativeConfiguration()}
 *   For this test you have to modify the default configuration such that the
 *   test principal is treated as administrative principal upon evaluation.
 *   Make sure the test passes and verify the expected result.
 *
 * </pre>
 */
public class L6_AdministratativeAccessTest extends AbstractSecurityTest {

    @Test
    public void testAdmininistrativePermissions() throws AccessDeniedException, CommitFailedException {
        // EXERCISE walk through the read access
        Tree rootTree = root.getTree("/");

        // EXERCISE walk through the add + remove
        NodeUtil child = new NodeUtil(rootTree).addChild("test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        root.commit();

        child.getTree().remove();
        root.commit();
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        // EXERCISE : change the configuration to make the test principal being treated as 'administrative' principal
        return super.getSecurityConfigParameters();
    }

    @Test
    public void testAdministrativeConfiguration() throws Exception {
        // EXERCISE once you have defined the right permission-eval configuration options
        // EXERCISE the test principal should be treated as 'administrative' principal and the test should pass.

        ContentSession testSession = createTestSession();
        try {
            Root testRoot = testSession.getLatestRoot();
            Tree rootTree = testRoot.getTree("/");

            // EXERCISE walk through the add + remove
            NodeUtil child = new NodeUtil(rootTree).addChild("test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            child.setString("prop", "val");
            testRoot.commit();

            child.getTree().remove();
            testRoot.commit();

        } finally {
            testSession.close();
        }

    }
}