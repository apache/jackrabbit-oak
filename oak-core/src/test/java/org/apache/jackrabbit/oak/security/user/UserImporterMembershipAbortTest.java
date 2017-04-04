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

import javax.jcr.nodetype.ConstraintViolationException;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.junit.Test;

public class UserImporterMembershipAbortTest extends UserImporterMembershipIgnoreTest {

    @Override
    String getImportBehavior() {
        return ImportBehavior.NAME_ABORT;
    }

    @Test(expected = ConstraintViolationException.class)
    public void testUnknownMember() throws Exception {
        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, unknownContentId)));
        importer.processReferences();
    }

    @Test(expected = ConstraintViolationException.class)
    public void testMixedMembers() throws Exception {
        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, unknownContentId, knownMemberContentId)));
        importer.processReferences();
    }
}