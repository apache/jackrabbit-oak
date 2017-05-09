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
package org.apache.jackrabbit.oak.security.user;

import java.util.Set;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AddMembersByIdAbortTest extends AbstractAddMembersByIdTest {

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(UserConfiguration.NAME,
                ConfigurationParameters.of(
                        ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_ABORT)
        );
    }

    @Test(expected = ConstraintViolationException.class)
    public void testNonExistingMember() throws Exception {
        addNonExistingMember();
    }

    @Test(expected = ConstraintViolationException.class)
    public void testExistingMemberWithoutAccess() throws Exception {
        addExistingMemberWithoutAccess();
    }

    @Test
    public void testCyclicMembership() throws Exception {
        memberGroup.addMember(testGroup);
        root.commit();

        try {
            Set<String> failed = testGroup.addMembers(memberGroup.getID());
            assertTrue(failed.isEmpty());
            root.commit();
            fail("cyclic group membership must be detected latest upon commit");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isConstraintViolation());
            assertEquals(31, e.getCode());
        }  catch (ConstraintViolationException e) {
            // success
        }
    }
}