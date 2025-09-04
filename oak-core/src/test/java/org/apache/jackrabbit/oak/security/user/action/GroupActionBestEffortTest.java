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
package org.apache.jackrabbit.oak.security.user.action;

import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class GroupActionBestEffortTest extends GroupActionTest {

    @Test
    public void testMembersAddedNonExisting() throws Exception {
        Set<String> nonExisting = Set.of("blinder", "passagier");

        testGroup.addMembers(nonExisting.toArray(new String[0]));

        verify(groupAction, times(1)).onMembersAdded(testGroup, nonExisting, Collections.emptySet(), root, getNamePathMapper());
    }

    @Test
    public void testMembersRemovedNonExisting() throws Exception {
        Set<String> nonExisting = Set.of("blinder", "passagier");

        testGroup.removeMembers(nonExisting.toArray(new String[0]));
        verify(groupAction, times(1)).onMembersRemoved(testGroup, Collections.emptySet(), nonExisting, root, getNamePathMapper());
    }

    @Override
    String getImportBehavior() {
        return ImportBehavior.NAME_BESTEFFORT;
    }
}
