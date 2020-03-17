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

import javax.jcr.Node;
import javax.jcr.Value;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.junit.Test;

public class CugImportIgnoreTest extends CugImportBaseTest {

    @Override
    protected String getImportBehavior() {
        return ImportBehavior.NAME_IGNORE;
    }

    @Test
    public void testCugInvalidPrincipals() throws Exception {
        Node targetNode = getTargetNode();
        targetNode.addMixin(CugConstants.MIX_REP_CUG_MIXIN);
        doImport(getTargetPath(), XML_CUG_POLICY);

        Node cugNode = targetNode.getNode(CugConstants.REP_CUG_POLICY);
        Value[] principalNames = cugNode.getProperty(CugConstants.REP_PRINCIPAL_NAMES).getValues();
        assertPrincipalNames(ImmutableSet.of(EveryonePrincipal.NAME), principalNames);

        getImportSession().save();
    }

    @Test
    public void testNodeWithCugInvalidPrincipals() throws Exception {
        doImport(getTargetPath(), XML_CHILD_WITH_CUG);

        Node cugNode = getTargetNode().getNode("child").getNode(CugConstants.REP_CUG_POLICY);
        Value[] principalNames = cugNode.getProperty(CugConstants.REP_PRINCIPAL_NAMES).getValues();
        assertPrincipalNames(ImmutableSet.of(EveryonePrincipal.NAME), principalNames);

        getImportSession().save();
    }
}