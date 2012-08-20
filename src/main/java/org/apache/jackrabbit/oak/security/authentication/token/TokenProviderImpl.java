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
import javax.jcr.Credentials;
import javax.jcr.PropertyType;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.apache.jackrabbit.oak.security.user.UserProviderImpl;
import org.apache.jackrabbit.oak.spi.security.user.PasswordUtility;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
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
    private final long tokenExpiration;

    public TokenProviderImpl(ContentSession contentSession, long tokenExpiration) {
        this.contentSession = contentSession;
        this.tokenExpiration = tokenExpiration;
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
            String userID = sc.getUserID();

            Root root = contentSession.getCurrentRoot();
            try {
                Tree userTree = getUserTree(contentSession, root, userID);
                if (userTree != null) {
                    Tree tokenParent = userTree.getChild(TOKENS_NODE_NAME);
                    if (tokenParent == null) {
                        tokenParent = userTree.addChild(TOKENS_NODE_NAME);
                        CoreValue primaryType = contentSession.getCoreValueFactory().createValue(TOKENS_NT_NAME);
                        tokenParent.setProperty(JcrConstants.JCR_PRIMARYTYPE, primaryType);
                    }

                    long creationTime = new Date().getTime();
                    Calendar creation = GregorianCalendar.getInstance();
                    creation.setTimeInMillis(creationTime);
                    String tokenName = Text.replace(ISO8601.format(creation), ":", ".");
                    Tree tokenTree = tokenParent.addChild(tokenName);

                    String key = generateKey(8);
                    String token = new StringBuilder(tokenTree.getPath()).append(DELIM).append(key).toString();

                    CoreValueFactory vf = contentSession.getCoreValueFactory();
                    tokenTree.setProperty(TOKEN_ATTRIBUTE_KEY, vf.createValue(PasswordUtility.buildPasswordHash(key)));
                    final long expirationTime = creationTime + tokenExpiration;
                    tokenTree.setProperty(TOKEN_ATTRIBUTE_EXPIRY, getExpirationValue(expirationTime));

                    Map<String, String> attributes;
                    for (String name : sc.getAttributeNames()) {
                        if (!TOKEN_ATTRIBUTE.equals(name)) {
                            String attr = sc.getAttribute(name).toString();
                            tokenTree.setProperty(name, vf.createValue(attr));
                        }
                    }
                    root.commit(DefaultConflictHandler.OURS);

                    // also set the new token to the simple credentials.
                    sc.setAttribute(TOKEN_ATTRIBUTE, token);
                    return new TokenInfoImpl(tokenTree, token);
                } else {
                    log.debug("Cannot create login token: No corresponding node for User " + userID + '.');
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
        Root root = contentSession.getCurrentRoot();
        int pos = token.indexOf(DELIM);
        String tokenPath = (pos == -1) ? token : token.substring(0, pos);
        Tree tokenTree = root.getTree(tokenPath);
        return (tokenTree == null) ? null : new TokenInfoImpl(tokenTree, token);
    }

    @Override
    public boolean removeToken(TokenInfo tokenInfo) {
        Tree tokenTree = getTokenTree(tokenInfo);
        if (tokenTree != null) {
            try {
                if (tokenTree.remove()) {
                    contentSession.getCurrentRoot().commit(DefaultConflictHandler.OURS);
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
            long expTime = tokenTree.getProperty(TOKEN_ATTRIBUTE_EXPIRY).getValue().getLong();
            if (expTime - loginTime <= tokenExpiration/2) {
                long expirationTime = loginTime + tokenExpiration;
                try {
                    tokenTree.setProperty(TOKEN_ATTRIBUTE_EXPIRY, getExpirationValue(expirationTime));
                    contentSession.getCurrentRoot().commit(DefaultConflictHandler.OURS);
                    return true;
                } catch (CommitFailedException e) {
                    log.warn("Error while resetting token expiration", e.getMessage());
                }
            }
        }
        return false;
    }


    //--------------------------------------------------------------------------

    private CoreValue getExpirationValue(long expirationTime) {
        Calendar cal = GregorianCalendar.getInstance();
        cal.setTimeInMillis(expirationTime);
        return contentSession.getCoreValueFactory().createValue(ISO8601.format(cal), PropertyType.DATE);
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

    private Tree getTokenTree(TokenInfo tokenInfo) {
        if (tokenInfo instanceof TokenInfoImpl) {
            return contentSession.getCurrentRoot().getTree(((TokenInfoImpl) tokenInfo).tokenPath);
        } else {
            return null;
        }
    }

    private static Tree getUserTree(ContentSession contentSession, Root root, String userID) {
        UserProvider userProvider = new UserProviderImpl(contentSession, root, null);
        return userProvider.getAuthorizable(userID);
    }

    //--------------------------------------------------------------------------

    private static class TokenInfoImpl implements TokenInfo {

        private final String token;
        private final String tokenPath;

        private final long expirationTime;
        private final String key;
        private Map<String, String> mandatoryAttributes;
        private Map<String, String> publicAttributes;


        private TokenInfoImpl(Tree tokenTree, String token) {
            this.token = token;
            this.tokenPath = tokenTree.getPath();

            PropertyState expTime = tokenTree.getProperty(TOKEN_ATTRIBUTE_EXPIRY);
            if (expTime == null) {
                expirationTime = Long.MIN_VALUE;
            } else {
                expirationTime = expTime.getValue().getLong();
            }

            PropertyState keyProp = tokenTree.getProperty(TOKEN_ATTRIBUTE_KEY);
            key = (keyProp == null) ? null : keyProp.getValue().getString();

            mandatoryAttributes = new HashMap<String, String>();
            publicAttributes = new HashMap<String, String>();
            for (PropertyState propertyState : tokenTree.getProperties()) {
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
