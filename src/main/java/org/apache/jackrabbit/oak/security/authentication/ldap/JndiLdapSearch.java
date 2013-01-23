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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JndiLdapSearch implements LdapSearch {

    private static final Logger log = LoggerFactory.getLogger(JndiLdapSearch.class);

    private final LdapSettings settings;
    private final Map<String,String> ldapEnvironment;

    public JndiLdapSearch(LdapSettings settings) {
        this.settings = settings;
        this.ldapEnvironment = createEnvironment(settings);
    }

    private static Map createEnvironment(LdapSettings settings) {
        Map<String,String> env = new HashMap<String,String>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        StringBuilder url = new StringBuilder();
        url.append("ldap://").append(settings.getHost()).append(':').append(settings.getPort());
        env.put(Context.PROVIDER_URL, url.toString());
        if (settings.isSecure()) {
            env.put(Context.SECURITY_PROTOCOL, "ssl");
        }
        String authDn = settings.getAuthDn();
        String authPw = settings.getAuthPw();
        if (authDn == null || authDn.length() == 0) {
            env.put(Context.SECURITY_AUTHENTICATION, "none");
        } else {
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            env.put(Context.SECURITY_PRINCIPAL, authDn);
            env.put(Context.SECURITY_CREDENTIALS, authPw);
        }
        return ImmutableMap.copyOf(env);
    }

    private Object parseAttributeValue(Attribute attribute) throws NamingException {
        int size = attribute.size();
        if (size > 1) {
            ArrayList<String> values = new ArrayList<String>();
            for (int k = 0; k < size; k++) {
                values.add(String.valueOf(attribute.get(k)));
            }
            return values;
        } else {
            return String.valueOf(attribute.get());
        }
    }

    private void initProperties(LdapUser user, Attributes attributes)
            throws NamingException {
        NamingEnumeration<? extends Attribute> namingEnumeration = attributes.getAll();
        Map<String, Object> properties = new HashMap<String, Object>();
        Map<String, String> syncMap = user instanceof LdapGroup ?
                settings.getGroupAttributes() : settings.getUserAttributes();
        while (namingEnumeration.hasMore()) {
            Attribute attribute = namingEnumeration.next();
            String key = attribute.getID();
            if (syncMap.containsKey(key)) {
                properties.put(syncMap.get(key), parseAttributeValue(attribute));
            }
        }
        user.setProperties(properties);
    }

    private List<SearchResult> search(String baseDN, String filter, int scope, String[] attributes)
            throws NamingException {
        // TODO: include scope param into query

        SearchControls constraints = new SearchControls();
        constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
        constraints.setCountLimit(0);
        constraints.setDerefLinkFlag(true);
        constraints.setTimeLimit(settings.getSearchTimeout());
        List<SearchResult> tmp = new ArrayList<SearchResult>();
        InitialDirContext context = null;
        try {
            context = new InitialDirContext(new Hashtable<String,String>(ldapEnvironment));
            NamingEnumeration<SearchResult> namingEnumeration = context.search(baseDN, filter, attributes, constraints);
            while (namingEnumeration.hasMore()) {
                tmp.add(namingEnumeration.next());
            }
        } catch (NamingException e) {
            log.error("LDAP search failed", e);
        } finally {
            if (context != null) {
                context.close();
            }
        }
        return tmp;
    }

    private String compileSearchFilter(String baseFilter, String searchExpression) {
        StringBuilder searchFilter = new StringBuilder("(&");

        // Add search expression first, it's typically fairly specific
        // so a server that evaluates clauses in order will perform well.
        // See https://bugs.day.com/bugzilla/show_bug.cgi?id=36917
        if (!(searchExpression == null || "".equals(searchExpression))) {
            if (!searchExpression.startsWith("(")) {
                searchFilter.append('(');
            }
            searchFilter.append(searchExpression);
            if (!searchExpression.endsWith(")")) {
                searchFilter.append(')');
            }
        }

        if (!(baseFilter == null || "".equals(baseFilter))) {
            if (!baseFilter.startsWith("(")) {
                searchFilter.append('(');
            }
            searchFilter.append(baseFilter);
            if (!baseFilter.endsWith(")")) {
                searchFilter.append(')');
            }
        }

        searchFilter.append(')');
        return searchFilter.toString();
    }

    private List<SearchResult> searchUser(String id)
            throws NamingException {
        Set<String> attributeSet = new HashSet<String>(settings.getUserAttributes().keySet());
        attributeSet.add(settings.getUserIdAttribute());
        String[] attributes = new String[attributeSet.size()];
        attributeSet.toArray(attributes);
        return search(settings.getUserRoot(),
                compileSearchFilter(settings.getUserFilter(), settings.getUserIdAttribute() + '=' + id),
                SearchControls.SUBTREE_SCOPE,
                attributes);
    }

    private List<SearchResult> searchGroups(String dn)
            throws NamingException {
        Set<String> attributeSet = new HashSet<String>(settings.getGroupAttributes().keySet());
        String[] attributes = new String[attributeSet.size()];
        attributeSet.toArray(attributes);
        return search(settings.getGroupRoot(),
                compileSearchFilter(settings.getGroupFilter(), settings.getGroupMembershipAttribute() + '=' + dn),
                SearchControls.SUBTREE_SCOPE,
                attributes);
    }

    private boolean findUser(LdapUser user, String id) {
        try {
            List<SearchResult> entries = searchUser(id);
            if (!entries.isEmpty()) {
                SearchResult entry = entries.get(0);
                user.setDN(entry.getNameInNamespace());
                initProperties(user, entry.getAttributes());
                return true;
            } else if (id.contains("\\")) {
                return findUser(user, id.substring(id.indexOf('\\') + 1));
            }
        } catch (NamingException e) {
            //TODO
        }
        return false;
    }

    @Override
    public boolean findUser(LdapUser user) {
        return findUser(user, user.getId());
    }

    @Override
    public Set<LdapGroup> findGroups(LdapUser user) {
        final HashSet<LdapGroup> groups = new HashSet<LdapGroup>();
        List<SearchResult> ldapEntries;
        try {
            ldapEntries = searchGroups(user.getDN());
            for (SearchResult entry : ldapEntries) {
                LdapGroup group = new LdapGroup(entry.getNameInNamespace(), this);
                groups.add(group);
                initProperties(group, entry.getAttributes());
            }
        } catch (NamingException e) {
            //TODO
        }
        return groups;
    }

    @Override
    public void authenticate(LdapUser user) throws LoginException {
        try {
            Hashtable<String,String> env = new Hashtable<String,String>(ldapEnvironment);
            env.put(Context.SECURITY_PRINCIPAL, user.getDN());
            env.put(Context.SECURITY_CREDENTIALS, user.getPassword());
            //TODO
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            new InitialDirContext(env).close();
        } catch (NamingException e) {
            throw new LoginException("Could not create initial LDAP context for user " + user.getDN() + ": " + e.getMessage());
        }
    }
}
