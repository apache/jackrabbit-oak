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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ValidatorNotDynamicTest extends ExternalIdentityValidatorTest {

    @Override
    public void before() throws Exception {
        super.before();
    }

    @Override
    protected boolean isDynamic() {
        return false;
    }

    private void setExternalPrincipalNames() throws Exception  {
        Root systemRoot = getSystemRoot();
        systemRoot.getTree(externalUserPath).setProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, ImmutableList.of("principalName"), Type.STRINGS);
        systemRoot.commit();

        root.refresh();
    }

    @Override
    @Test
    public void testRemoveExternalPrincipalNames() throws Exception {
        setExternalPrincipalNames();

        super.testRemoveExternalPrincipalNames();
    }

    @Override
    @Test
    public void testRemoveExternalPrincipalNamesAsSystem() throws Exception {
        setExternalPrincipalNames();

        super.testRemoveExternalPrincipalNamesAsSystem();
    }

    @Override
    @Test
    public void testModifyExternalPrincipalNames() throws Exception {
        setExternalPrincipalNames();

        super.testModifyExternalPrincipalNames();
    }

    @Override
    @Test
    public void testModifyExternalPrincipalNamesAsSystem() throws Exception {
        setExternalPrincipalNames();

        super.testModifyExternalPrincipalNamesAsSystem();
    }


    @Override
    @Test
    public void testRemoveRepExternalId() throws Exception {
        try {
            root.getTree(externalUserPath).removeProperty(ExternalIdentityConstants.REP_EXTERNAL_ID);
            root.commit();

            fail("Removal of rep:externalId must be detected in the default setup.");
        } catch (CommitFailedException e) {
            // success: verify nature of the exception
            assertTrue(e.isConstraintViolation());
            assertEquals(74, e.getCode());
        }
    }

    @Override
    @Test
    public void testRemoveRepExternalIdAsSystem() throws Exception {
        Root systemRoot = getSystemRoot();
        NodeUtil n = new NodeUtil(systemRoot.getTree(externalUserPath));

        n.removeProperty(ExternalIdentityConstants.REP_EXTERNAL_ID);
        systemRoot.commit();
    }
}