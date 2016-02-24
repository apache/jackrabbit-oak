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

import javax.jcr.Node;
import javax.jcr.Value;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Testing user import with default {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior}
 * and pw-history content: test that the history is imported irrespective of the
 * configuration.
 */
public class UserImportHistoryTest extends AbstractImportTest {

    @Override
    protected String getTargetPath() {
        return USERPATH;
    }

    @Override
    protected String getImportBehavior() {
        return null;
    }

    /**
     * @since Oak 1.3.3
     */
    @Test
    public void testImportUserWithPwdHistory() throws Exception {
        // import user
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"y\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                "      <sv:value>rep:User</sv:value>" +
                "   </sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\">" +
                "      <sv:value>41529076-9594-360e-ae48-5922904f345d</sv:value>" +
                "   </sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\">" +
                "      <sv:value>pw</sv:value>" +
                "   </sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\">" +
                "      <sv:value>yPrincipal</sv:value>" +
                "   </sv:property>" +
                "   <sv:node sv:name=\"" + UserConstants.REP_PWD + "\">" +
                "      <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                "         <sv:value>" + UserConstants.NT_REP_PASSWORD + "</sv:value>" +
                "      </sv:property>" +
                "      <sv:property sv:name=\"" + UserConstants.REP_PWD_HISTORY + "\" sv:type=\"String\" sv:multiple=\"true\">" +
                "         <sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value>" +
                "      </sv:property>" +
                "   </sv:node>" +
                "</sv:node>";

        doImport(USERPATH, xml);

        Authorizable authorizable = getUserManager().getAuthorizable("y");
        Node userNode = getImportSession().getNode(authorizable.getPath());
        assertTrue(userNode.hasNode(UserConstants.REP_PWD));

        Node pwdNode = userNode.getNode(UserConstants.REP_PWD);
        assertTrue(pwdNode.hasProperty(UserConstants.REP_PWD_HISTORY));
        Value[] vs = pwdNode.getProperty(UserConstants.REP_PWD_HISTORY).getValues();
        assertEquals(1, vs.length);
        assertEquals("{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375", vs[0].getString());
    }
}
