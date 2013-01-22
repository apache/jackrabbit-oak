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

import java.util.HashMap;
import java.util.Map;

public final class LdapSettings {

    //TODO support autocreate.user.membership
    //TODO support autocreate.path

    public final static String KEY_HOST = "host";
    public final static String KEY_PORT = "port";
    public final static String KEY_SECURE = "secure";
    public final static String KEY_AUTHDN = "authDn";
    public final static String KEY_AUTHPW = "authPw";
    public final static String KEY_SEARCHTIMEOUT = "searchTimeout";
    public final static String KEY_USERROOT = "userRoot";
    public final static String KEY_USERFILTER = "userFilter";
    public final static String KEY_USERIDATTRIBUTE = "userIdAttribute";
    public final static String KEY_GROUPROOT = "groupRoot";
    public final static String KEY_GROUPFILTER = "groupFilter";
    public final static String KEY_GROUPMEMBERSHIPATTRIBUTE = "groupMembershipAttribute";
    public final static String KEY_GROUPNAMEATTRIBUTE = "groupNameAttribute";
    public final static String KEY_AUTOCREATEPATH = "autocreate.path";
    public final static String KEY_AUTOCREATEUSER = "autocreate.user.";
    public final static String KEY_AUTOCREATEGROUP = "autocreate.group.";

    //Connection settings
    private String host;
    private int port = 389;
    private boolean secure = false;
    private String authDn = "";
    private String authPw = "";
    private int searchTimeout = 60000;

    //authentication settings
    private String userRoot = "";
    private String userFilter = "(objectclass=person)";
    private String userIdAttribute = "uid";
    private String groupRoot = "";
    private String groupFilter = "(objectclass=groupOfUniqueNames)";
    private String groupMembershipAttribute = "uniquemember";
    private String groupNameAttribute = "cn";

    //synchronization
    private boolean splitPath = false;
    private final Map<String, String> userAttributes = new HashMap<String, String>();
    private final Map<String, String> groupAttributes = new HashMap<String, String>();

    public LdapSettings(Map<String, ?> options) {
        if (options.containsKey(KEY_HOST)) {
            this.host = (String) options.get(KEY_HOST);
        }
        if (options.containsKey(KEY_PORT)) {
            String s = (String) options.get(KEY_PORT);
            if (s != null && s.length() > 0) {
                this.port = Integer.parseInt(s);
            }
        }
        if (options.containsKey(KEY_SECURE)) {
            String s = (String) options.get(KEY_SECURE);
            if (s != null && s.length() > 0) {
                this.secure = Boolean.parseBoolean(s);
            }
        }
        if (options.containsKey(KEY_AUTHDN)) {
            this.authDn = (String) options.get(KEY_AUTHDN);
        }
        if (options.containsKey(KEY_AUTHPW)) {
            this.authPw = (String) options.get(KEY_AUTHPW);
        }
        if (options.containsKey(KEY_SEARCHTIMEOUT)) {
            String s = (String) options.get(KEY_SEARCHTIMEOUT);
            if (s != null && s.length() > 0) {
                this.searchTimeout = Integer.parseInt(s);
            }
        }
        if (options.containsKey(KEY_USERROOT)) {
            this.userRoot = (String) options.get(KEY_USERROOT);
        }
        if (options.containsKey(KEY_USERFILTER)) {
            this.userFilter = (String) options.get(KEY_USERFILTER);
        }
        if (options.containsKey(KEY_USERIDATTRIBUTE)) {
            this.userIdAttribute = (String) options.get(KEY_USERIDATTRIBUTE);
        }
        if (options.containsKey(KEY_GROUPROOT)) {
            this.groupRoot = (String) options.get(KEY_GROUPROOT);
        }
        if (options.containsKey(KEY_GROUPFILTER)) {
            this.groupFilter = (String) options.get(KEY_GROUPFILTER);
        }
        if (options.containsKey(KEY_GROUPMEMBERSHIPATTRIBUTE)) {
            this.groupMembershipAttribute = (String) options.get(KEY_GROUPMEMBERSHIPATTRIBUTE);
        }
        if (options.containsKey(KEY_GROUPNAMEATTRIBUTE)) {
            this.groupNameAttribute = (String) options.get(KEY_GROUPNAMEATTRIBUTE);
        }
        if (options.containsKey(KEY_AUTOCREATEPATH)) {
            this.splitPath = "splitdn".equals(options.get(KEY_AUTOCREATEPATH));
        }
        for (String key : options.keySet()) {
            if (key.startsWith(KEY_AUTOCREATEUSER)) {
                this.userAttributes.put(key.substring(KEY_AUTOCREATEUSER.length()), (String) options.get(key));
            }
            if (key.startsWith(KEY_AUTOCREATEGROUP)) {
                this.groupAttributes.put(key.substring(KEY_AUTOCREATEGROUP.length()), (String) options.get(key));
            }
        }
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public boolean isSecure() {
        return this.secure;
    }

    public String getAuthDn() {
        return this.authDn;
    }

    public String getAuthPw() {
        return this.authPw;
    }

    public int getSearchTimeout() {
        return this.searchTimeout;
    }

    public String getUserRoot() {
        return this.userRoot;
    }

    public String getUserFilter() {
        return this.userFilter;
    }

    public String getUserIdAttribute() {
        return this.userIdAttribute;
    }

    public String getGroupRoot() {
        return this.groupRoot;
    }

    public String getGroupFilter() {
        return this.groupFilter;
    }

    public String getGroupMembershipAttribute() {
        return this.groupMembershipAttribute;
    }

    public String getGroupNameAttribute() {
        return this.groupNameAttribute;
    }

    public boolean isSplitPath() {
        return this.splitPath;
    }

    public Map<String, String> getUserAttributes() {
        return this.userAttributes;
    }

    public Map<String, String> getGroupAttributes() {
        return this.groupAttributes;
    }
}
