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

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

public class ValidatorNoProtectionTest extends ExternalIdentityValidatorTest {

    @Override
    public void before() throws Exception {
        super.before();

        ConfigurationParameters params = ConfigurationParameters.of(externalPrincipalConfiguration.getParameters(), ConfigurationParameters.of(ExternalIdentityConstants.PARAM_PROTECT_EXTERNAL_IDS, false));
        externalPrincipalConfiguration.setParameters(params);
    }

    @Override
    @Test
    public void testRepExternalIdMultiple() throws Exception {
        Root systemRoot = getSystemRoot();
        NodeUtil n = new NodeUtil(systemRoot.getTree(testUserPath));
        n.setStrings(ExternalIdentityConstants.REP_EXTERNAL_ID, "id", "id2");
        systemRoot.commit();
    }

    @Override
    @Test
    public void testRepExternalIdType() throws Exception {
        Root systemRoot = getSystemRoot();
        Tree userTree = systemRoot.getTree(testUserPath);

        Map<Type, Object> valMap = ImmutableMap.<Type, Object>of(
                Type.BOOLEAN, Boolean.TRUE,
                Type.LONG, new Long(1234),
                Type.NAME, "id"
        );
        for (Type t : valMap.keySet()) {
            Object val = valMap.get(t);
            try {
                userTree.setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, val, t);
                systemRoot.commit();
            } finally {
                systemRoot.refresh();
            }
        }
    }

    @Override
    public void testAddRepExternalId() throws Exception {
        root.getTree(testUserPath).setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, "id");
        root.commit();
    }

    @Override
    @Test
    public void testModifyRepExternalId() throws Exception {
        root.getTree(externalUserPath).setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, "anotherValue");
        root.commit();
    }

    @Override
    public void testRemoveRepExternalIdWithoutPrincipalNames() throws Exception {
        root.getTree(testUserPath).setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, "id");
        root.commit();

        root.getTree(testUserPath).removeProperty(ExternalIdentityConstants.REP_EXTERNAL_ID);
        root.commit();
    }
}