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
package org.apache.jackrabbit.api;

import java.util.Map;

import javax.jcr.Credentials;
import javax.jcr.LoginException;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

/**
 * The Jackrabbit repository interface. This interface contains the
 * Jackrabbit-specific extensions to the JCR {@link Repository} interface.
 */
public interface JackrabbitRepository extends Repository {

    /**
     * Key to a <code>boolean</code> descriptor. Returns <code>true</code> if
     * and only if user management is supported.
     */
    String OPTION_USER_MANAGEMENT_SUPPORTED = "option.user.management.supported";

    /**
     * Key to a <code>boolean</code> descriptor. Returns <code>true</code> if
     * and only if principal management is supported.
     */
    String OPTION_PRINCIPAL_MANAGEMENT_SUPPORTED = "option.principal.management.supported";

    /**
     * Key to a <code>boolean</code> descriptor. Returns <code>true</code> if
     * and only if privilege management is supported.
     */
    String OPTION_PRIVILEGE_MANAGEMENT_SUPPORTED = "option.privilege.management.supported";

    /**
     * Equivalent to {@code login(credentials, workspaceName)} except that the returned
     * Session instance contains the given extra session attributes in addition to any
     * included in the given Credentials instance. Attribute names from the credentials
     * and the attribute map must not overlap. In case of an overlap implementation
     * may throw an <code>RepositoryException</code>.
     * <p>
     * The attributes are implementation-specific and may affect the behavior of the returned
     * session. Unlike credentials attributes, these separately passed session attributes
     * are guaranteed not to affect the authentication of the client.
     * <p>
     * An implementation that does not support a particular session attribute is expected
     * to ignore it and not make it available through the returned session. A client that
     * depends on specific behavior defined by a particular attribute can check whether
     * the returned session contains that attribute to verify whether the underlying
     * repository implementation supports that feature.
     *
     * @param credentials the credentials of the user
     * @param workspaceName the name of a workspace
     * @param attributes implementation-specific session attributes
     * @return a valid session for the user to access the repository
     * @throws LoginException if authentication or authorization for the specified workspace fails
     * @throws NoSuchWorkspaceException if the specified workspace is not recognized
     * @throws RepositoryException if another error occurs
     */
    Session login(Credentials credentials, String workspaceName, Map<String, Object> attributes)
            throws LoginException, NoSuchWorkspaceException, RepositoryException;

    /**
     * Shuts down the repository. A Jackrabbit repository instance contains
     * a acquired resources and cached data that needs to be released and
     * persisted when the repository is no longer used. This method handles
     * all these shutdown tasks and <em>must</em> therefore be called by the
     * client application once the repository instance is no longer used.
     * <p>
     * Possible errors are logged rather than thrown as exceptions as there
     * is little that a client application could do in such a case.
     */
    void shutdown();

}
