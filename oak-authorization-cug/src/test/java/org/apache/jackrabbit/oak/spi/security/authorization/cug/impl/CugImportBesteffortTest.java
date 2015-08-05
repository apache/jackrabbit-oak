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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.value.ValueHelper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CugImportBesteffortTest extends CugImportBaseTest {

    private final Set<String> PRINCIPAL_NAMES = Sets.newHashSet(EveryonePrincipal.NAME, TEST_GROUP_PRINCIPAL_NAME);

    @Override
    protected String getImportBehavior() {
        return ImportBehavior.NAME_BESTEFFORT;
    }

    @Test
    public void testCugInvalidPrincipals() throws Exception {
        Node targetNode = getTargetNode();
        targetNode.addMixin(CugConstants.MIX_REP_CUG_MIXIN);
        doImport(getTargetPath(), XML_CUG_POLICY);

        Node cugNode = targetNode.getNode(CugConstants.REP_CUG_POLICY);
        Value[] principalNames = cugNode.getProperty(CugConstants.REP_PRINCIPAL_NAMES).getValues();

        assertPrincipalNames(PRINCIPAL_NAMES, principalNames);

        getImportSession().save();
    }

    @Test
    public void testNodeWithCugInvalidPrincipals() throws Exception {
        doImport(getTargetPath(), XML_CHILD_WITH_CUG);

        Node cugNode = getTargetNode().getNode("child").getNode(CugConstants.REP_CUG_POLICY);
        Value[] principalNames = cugNode.getProperty(CugConstants.REP_PRINCIPAL_NAMES).getValues();

        assertPrincipalNames(PRINCIPAL_NAMES, principalNames);

        getImportSession().save();
    }
}