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
package org.apache.jackrabbit.oak.security.authentication.token;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.user.PasswordUtility;
import org.apache.jackrabbit.oak.spi.security.user.Type;
import org.apache.jackrabbit.oak.spi.security.user.UserContext;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TokenProvider... TODO
 */
public class TokenProviderImpl implements TokenProvider {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(TokenProviderImpl.class);

    /**
     * Constant for the token attribute passed with simple credentials to
     * trigger the generation of a new token.
     */
    private static final String TOKEN_ATTRIBUTE = ".token";

    private static final String TOKEN_ATTRIBUTE_EXPIRY = TOKEN_ATTRIBUTE + ".exp";
    private static final String TOKEN_ATTRIBUTE_KEY = TOKEN_ATTRIBUTE + ".key";
    private static final String TOKENS_NODE_NAME = ".tokens";
    private static final String TOKENS_NT_NAME = JcrConstants.NT_UNSTRUCTURED;

    private static final int STATUS_VALID = 0;
    private static final int STATUS_EXPIRED = 1;
    private static final int STATUS_MISMATCH = 2;

    private static final char DELIM = '_';

    private final ContentSession contentSession;
    private final Root root;
    private final UserProvider userProvider;
    private final long tokenExpiration;

    public TokenProviderImpl(ContentSession contentSession, long tokenExpiration, UserContext userContext) {
        this.contentSession = contentSession;
        this.root = contentSession.getLatestRoot();
        this.tokenExpiration = tokenExpiration;

        this.userProvider = userContext.getUserProvider(contentSession, root);
    }

    //------------------------------------------------------< TokenProvider >---
    @Override
    public boolean doCreateToken(Credentials credentials) {
        if (credentials instanceof SimpleCredentials) {
            SimpleCredentials sc = (SimpleCredentials) credentials;
            Object attr = sc.getAttribute(TOKEN_ATTRIBUTE);
            return (attr != null && "".equals(attr.toString()));
        } else {
            return false;
        }
    }

    @Override
    public TokenInfo createToken(Credentials credentials) {
        if (credentials instanceof SimpleCredentials) {
            final SimpleCredentials sc = (SimpleCredentials) credentials;
            String userId = sc.getUserID();

            CoreValueFactory valueFactory = contentSession.getCoreValueFactory();
            try {
                Tree userTree = userProvider.getAuthorizable(userId, Type.USER);
                if (userTree != null) {
                    NodeUtil userNode = new NodeUtil(userTree, valueFactory);
                    NodeUtil tokenParent = userNode.getChild(TOKENS_NODE_NAME);
                    if (tokenParent == null) {
                        tokenParent = userNode.addChild(TOKENS_NODE_NAME, TOKENS_NT_NAME);
                    }

                    long creationTime = new Date().getTime();
                    Calendar creation = GregorianCalendar.getInstance();
                    creation.setTimeInMillis(creationTime);
                    String tokenName = Text.replace(ISO8601.format(creation), ":", ".");

                    NodeUtil tokenNode = tokenParent.addChild(tokenName, TOKENS_NT_NAME);

                    String key = generateKey(8);
                    String token = new StringBuilder(tokenNode.getTree().getPath()).append(DELIM).append(key).toString();

                    String tokenHash = PasswordUtility.buildPasswordHash(key);
                    tokenNode.setString(TOKEN_ATTRIBUTE_KEY, tokenHash);
                    final long expirationTime = creationTime + tokenExpiration;
                    tokenNode.setDate(TOKEN_ATTRIBUTE_EXPIRY, expirationTime);

                    Map<String, String> attributes;
                    for (String name : sc.getAttributeNames()) {
                        if (!TOKEN_ATTRIBUTE.equals(name)) {
                            String attr = sc.getAttribute(name).toString();
                            tokenNode.setString(name, attr);
                        }
                    }
                    root.commit();

                    // also set the new token to the simple credentials.
                    sc.setAttribute(TOKEN_ATTRIBUTE, token);
                    return new TokenInfoImpl(tokenNode, token, userId);
                } else {
                    log.debug("Cannot create login token: No corresponding node for User " + userId + '.');
                }

            } catch (NoSuchAlgorithmException e) {
                log.debug("Failed to create login token ", e.getMessage());
            } catch (UnsupportedEncodingException e) {
                log.debug("Failed to create login token ", e.getMessage());
            } catch (CommitFailedException e) {
                log.debug("Failed to create login token ", e.getMessage());
            }
        }

        return null;
    }

    @Override
    public TokenInfo getTokenInfo(String token) {
        int pos = token.indexOf(DELIM);
        String tokenPath = (pos == -1) ? token : token.substring(0, pos);
        Tree tokenTree = root.getTree(tokenPath);
        String userId = getUserId(tokenTree);
        if (tokenTree == null || userId == null) {
            return null;
        } else {
            return new TokenInfoImpl(new NodeUtil(tokenTree, contentSession), token, userId);
        }
    }

    @Override
    public boolean removeToken(TokenInfo tokenInfo) {
        Tree tokenTree = getTokenTree(tokenInfo);
        if (tokenTree != null) {
            try {
                if (tokenTree.remove()) {
                    root.commit();
                    return true;
                }
            } catch (CommitFailedException e) {
                log.debug("Error while removing expired token", e.getMessage());
            }
        }
        return false;
    }

    @Override
    public boolean resetTokenExpiration(TokenInfo tokenInfo, long loginTime) {
        Tree tokenTree = getTokenTree(tokenInfo);
        if (tokenTree != null) {
            NodeUtil tokenNode = new NodeUtil(tokenTree, contentSession);
            long expTime = tokenNode.getLong(TOKEN_ATTRIBUTE_EXPIRY, 0);
            if (expTime - loginTime <= tokenExpiration/2) {
                long expirationTime = loginTime + tokenExpiration;
                try {
                    tokenNode.setDate(TOKEN_ATTRIBUTE_EXPIRY, expirationTime);
                    root.commit();
                    return true;
                } catch (CommitFailedException e) {
                    log.warn("Error while resetting token expiration", e.getMessage());
                }
            }
        }
        return false;
    }


    //--------------------------------------------------------------------------

    private static String generateKey(int size) {
        SecureRandom random = new SecureRandom();
        byte key[] = new byte[size];
        random.nextBytes(key);

        StringBuilder res = new StringBuilder(key.length * 2);
        for (byte b : key) {
            res.append(Text.hexTable[(b >> 4) & 15]);
            res.append(Text.hexTable[b & 15]);
        }
        return res.toString();
    }

    @CheckForNull
    private Tree getTokenTree(TokenInfo tokenInfo) {
        if (tokenInfo instanceof TokenInfoImpl) {
            return root.getTree(((TokenInfoImpl) tokenInfo).tokenPath);
        } else {
            return null;
        }
    }

    @CheckForNull
    private String getUserId(Tree tokenTree) {
        if (tokenTree != null) {
            Tree userTree = tokenTree.getParent().getParent();
            return userProvider.getAuthorizableId(userTree);
        }

        return null;
    }

    //--------------------------------------------------------------------------

    private static class TokenInfoImpl implements TokenInfo {

        private final String token;
        private final String tokenPath;
        private final String userId;

        private final long expirationTime;
        private final String key;

        private final Map<String, String> mandatoryAttributes;
        private final Map<String, String> publicAttributes;


        private TokenInfoImpl(NodeUtil tokenNode, String token, String userId) {
            this.token = token;
            this.tokenPath = tokenNode.getTree().getPath();
            this.userId = userId;

            expirationTime = tokenNode.getLong(TOKEN_ATTRIBUTE_EXPIRY, Long.MIN_VALUE);
            key = tokenNode.getString(TOKEN_ATTRIBUTE_KEY, null);

            mandatoryAttributes = new HashMap<String, String>();
            publicAttributes = new HashMap<String, String>();
            for (PropertyState propertyState : tokenNode.getTree().getProperties()) {
                String name = propertyState.getName();
                String value = propertyState.getValue().getString();
                if (isMandatoryAttribute(name)) {
                    mandatoryAttributes.put(name, value);
                } else if (isInfoAttribute(name)) {
                    // info attribute
                    publicAttributes.put(name, value);
                } // else: jcr specific property
            }
        }

        //------------------------------------------------------< TokenInfo >---

        @Override
        public String getUserId() {
            return userId;
        }

        @Override
        public String getToken() {
            return token;
        }

        @Override
        public boolean isExpired(long loginTime) {
            return expirationTime < loginTime;
        }

        @Override
        public boolean matches(TokenCredentials tokenCredentials) {
            if (key == null || !PasswordUtility.isSame(key, tokenCredentials.getToken())) {
                return false;
            }

            for (String name : mandatoryAttributes.keySet()) {
                String expectedValue = mandatoryAttributes.get(name);
                if (!expectedValue.equals(tokenCredentials.getAttribute(name))) {
                    return false;
                }
            }

            // update set of informative attributes on the credentials
            // based on the properties present on the token node.
            Collection<String> attrNames = Arrays.asList(tokenCredentials.getAttributeNames());
            for (String name : publicAttributes.keySet()) {
                if (!attrNames.contains(name)) {
                    tokenCredentials.setAttribute(name, publicAttributes.get(name).toString());

                }
            }
            return true;
        }

        @Override
        public Map<String, String> getPrivateAttributes() {
            return Collections.unmodifiableMap(mandatoryAttributes);
        }

        @Override
        public Map<String, String> getPublicAttributes() {
            return Collections.unmodifiableMap(publicAttributes);
        }

        /**
         * Returns {@code true} if the specified {@code attributeName}
         * starts with or equals {@link #TOKEN_ATTRIBUTE}.
         *
         * @param attributeName
         * @return {@code true} if the specified {@code attributeName}
         * starts with or equals {@link #TOKEN_ATTRIBUTE}.
         */
        private static boolean isMandatoryAttribute(String attributeName) {
            return attributeName != null && attributeName.startsWith(TOKEN_ATTRIBUTE);
        }

        /**
         * Returns {@code false} if the specified attribute name doesn't have
         * a 'jcr' or 'rep' namespace prefix; {@code true} otherwise. This is
         * a lazy evaluation in order to avoid testing the defining node type of
         * the associated jcr property.
         *
         * @param propertyName
         * @return {@code true} if the specified property name doesn't seem
         * to represent repository internal information.
         */
        private static boolean isInfoAttribute(String propertyName) {
            String prefix = Text.getNamespacePrefix(propertyName);
            return !"jcr".equals(prefix) && !"rep".equals(prefix);
        }
    }
}
