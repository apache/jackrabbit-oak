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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.security.AccessControlPolicy;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_NAME;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_POLICY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ImportIgnoreTest extends ImportBaseTest {

    @Override
    String getImportBehavior() {
        return ImportBehavior.NAME_IGNORE;
    }

    @Test
    public void testTransientPrincipal() throws Exception {
        User transientSystemUser = getUserManager().createSystemUser("transientSystemUser", INTERMEDIATE_PATH);
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>"+transientSystemUser.getPrincipal().getName()+"</sv:value></sv:property>" +
                "</sv:node>";
        doImport(transientSystemUser.getPath(), xml);

        assertTrue(getSession().getNode(transientSystemUser.getPath()).hasNode(REP_PRINCIPAL_POLICY));
        Node policy = getSession().getNode(transientSystemUser.getPath()).getNode(REP_PRINCIPAL_POLICY);
        assertTrue(policy.hasProperty(REP_PRINCIPAL_NAME));

        // but looking up policy doesn't work because of transient principal.
        AccessControlPolicy[] policies = getAccessControlManager().getPolicies(transientSystemUser.getPrincipal());
        assertEquals(0, policies.length);
    }
}