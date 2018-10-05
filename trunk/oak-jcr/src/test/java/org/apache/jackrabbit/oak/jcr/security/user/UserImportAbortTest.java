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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.util.ArrayList;
import java.util.List;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Testing {@link ImportBehavior#ABORT} for user import
 */
public class UserImportAbortTest extends AbstractImportTest {

    @Override
    protected String getImportBehavior() {
        return ImportBehavior.NAME_ABORT;
    }

    @Override
    protected String getTargetPath() {
        return USERPATH;
    }

    @Test
    public void testImportInvalidImpersonationAbort() throws Exception {
        List<String> invalid = new ArrayList<String>();
        invalid.add("administrators"); // a group
        invalid.add("t"); // principal of the user itself.

        for (String principalName : invalid) {
            String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:impersonators\" sv:type=\"String\"><sv:value>" +principalName+ "</sv:value></sv:property>" +
                    "</sv:node>";
            try {
                doImport(getTargetPath(), xml);
                fail("UserImporter.ImportBehavior.ABORT -> importing invalid impersonators must throw.");
            } catch (RepositoryException e) {
                // success
            } finally {
                getImportSession().refresh(false);
            }
        }
    }

    @Test
    public void testImportNonExistingImpersonationAbort() throws Exception {
        String nonExistingPrincipalName = "anybody";
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:impersonators\" sv:type=\"String\"><sv:value>" +nonExistingPrincipalName+ "</sv:value></sv:property>" +
                "</sv:node>";
        try {
            doImport(getTargetPath(), xml);
            fail("UserImporter.ImportBehavior.ABORT -> importing invalid impersonators must throw.");
        } catch (RepositoryException e) {
            // success
        } finally {
            getImportSession().refresh(false);
        }
    }
}
