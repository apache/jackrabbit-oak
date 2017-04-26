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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AddMembersByIdIgnoreTest extends AbstractAddMembersByIdTest {

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(UserConfiguration.NAME,
                ConfigurationParameters.of(
                        ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_IGNORE)
        );
    }

    @Test
    public void testNonExistingMember() throws Exception {
        Set<String> failed = addNonExistingMember();
        assertEquals(ImmutableSet.copyOf(NON_EXISTING_IDS), failed);
    }

    @Test
    public void testExistingMemberWithoutAccess() throws Exception {
        Set<String> failed = addExistingMemberWithoutAccess();
        assertEquals(ImmutableSet.of(memberGroup.getID()), failed);

        root.refresh();
        assertFalse(testGroup.isMember(memberGroup));
    }
}