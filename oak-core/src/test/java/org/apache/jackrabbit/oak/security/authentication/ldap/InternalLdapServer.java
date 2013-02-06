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
package org.apache.jackrabbit.oak.security.authentication.ldap;

import java.io.File;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.ldap.LdapContext;

import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.directory.server.unit.AbstractServerTest;

class InternalLdapServer extends AbstractServerTest {

    public static final String GROUP_MEMBER_ATTR = "member";
    public static final String GROUP_CLASS_ATTR = "groupOfNames";

    public static final String ADMIN_PW = "secret";

    public void setUp() throws Exception {
        super.setUp();
        doDelete = true;
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected void configureDirectoryService() throws Exception {
        directoryService.setWorkingDirectory(new File("target", "apacheds"));
        doDelete(directoryService.getWorkingDirectory());
    }

    public int getPort() {
        return port;
    }

    public String addUser(String firstName, String lastName, String userId, String password)
            throws Exception {
        String cn = firstName + ' ' + lastName;
        String dn = buildDn(cn, false);
        StringBuilder entries = new StringBuilder();
        entries.append("dn: ").append(dn).append('\n')
                .append("objectClass: inetOrgPerson\n").append("cn: ").append(cn)
                .append('\n').append("sn: ").append(lastName)
                .append('\n').append("givenName:").append(firstName)
                .append('\n').append("uid: ").append(userId)
                .append('\n').append("userPassword: ").append(password).append("\n\n");
        injectEntries(entries.toString());
        return dn;
    }

    public String addGroup(String name) throws Exception {
        String dn = buildDn(name, true);
        StringBuilder entries = new StringBuilder();
        entries.append("dn: ").append(dn).append('\n').append("objectClass: ")
                .append(GROUP_CLASS_ATTR).append('\n').append(GROUP_MEMBER_ATTR)
                .append(":\n").append("cn: ").append(name).append("\n\n");
        injectEntries(entries.toString());
        return dn;
    }

    public void addMember(String groupDN, String memberDN) throws Exception {
        LdapContext ctxt = getWiredContext();
        BasicAttributes attrs = new BasicAttributes();
        attrs.put("member", memberDN);
        ctxt.modifyAttributes(groupDN, DirContext.ADD_ATTRIBUTE, attrs);
    }

    public void removeMember(String groupDN, String memberDN) throws Exception {
        LdapContext ctxt = getWiredContext();
        BasicAttributes attrs = new BasicAttributes();
        attrs.put("member", memberDN);
        ctxt.modifyAttributes(groupDN, DirContext.REMOVE_ATTRIBUTE, attrs);
    }

    private static String buildDn(String name, boolean isGroup) {
        StringBuilder dn = new StringBuilder();
        dn.append(name).append(',');
        if (isGroup) {
            dn.append(ServerDNConstants.GROUPS_SYSTEM_DN);
        } else {
            dn.append(ServerDNConstants.USERS_SYSTEM_DN);
        }
        return dn.toString();
    }
}
