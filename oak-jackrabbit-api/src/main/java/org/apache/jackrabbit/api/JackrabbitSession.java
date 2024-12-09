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

import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;

import javax.jcr.Item;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.Session;
import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Jackrabbit specific extension of the JCR {@link javax.jcr.Session} interface.
 */
@ProviderType
public interface JackrabbitSession extends Session {

    /**
     * A constant representing the {@code add_property} action string, used to
     * determine if this {@code Session} has permission to add a new property.
     *
     * @see #hasPermission(String, String...)
     */
    String ACTION_ADD_PROPERTY = "add_property";

    /**
     * A constant representing the {@code modify_property} action string, used to
     * determine if this {@code Session} has permission to modify a property.
     *
     * @see #hasPermission(String, String...)
     */
    String ACTION_MODIFY_PROPERTY = "modify_property";

    /**
     * A constant representing the {@code remove_property} action string, used to
     * determine if this {@code Session} has permission to remove a property.
     *
     * @see #hasPermission(String, String...)
     */
    String ACTION_REMOVE_PROPERTY = "remove_property";

    /**
     * A constant representing the {@code remove_node} action string, used to
     * determine if this {@code Session} has permission to remove a node.
     *
     * @see #hasPermission(String, String...)
     */
    String ACTION_REMOVE_NODE = "remove_node";

    /**
     * A constant representing the {@code node_type_management} action string,
     * used to determine if this {@code Session} has permission to write
     * node type information of a node.
     *
     * @see #hasPermission(String, String...)
     */
    String ACTION_NODE_TYPE_MANAGEMENT = "node_type_management";

    /**
     * A constant representing the {@code versioning} action string,
     * used to determine if this {@code Session} has permission to perform
     * version operations on a node.
     *
     * @see #hasPermission(String, String...)
     */
    String ACTION_VERSIONING = "versioning";

    /**
     * A constant representing the {@code locking} action string,
     * used to determine if this {@code Session} has permission to lock or
     * unlock a node.
     *
     * @see #hasPermission(String, String...)
     */
    String ACTION_LOCKING = "locking";

    /**
     * A constant representing the {@code read_access_control} action string,
     * used to determine if this {@code Session} has permission to read
     * access control content at the given path.
     *
     * @see #hasPermission(String, String...)
     */
    String ACTION_READ_ACCESS_CONTROL = "read_access_control";

    /**
     * A constant representing the {@code modify_access_control} action string,
     * used to determine if this {@code Session} has permission to modify
     * access control content at the given path.
     *
     * @see #hasPermission(String, String...)
     */
    String ACTION_MODIFY_ACCESS_CONTROL = "modify_access_control";

    /**
     * A constant representing the {@code user_management} action string,
     * used to determine if this {@code Session} has permission to perform
     * user management operations at the given path.
     *
     * @see #hasPermission(String, String...)
     */
    String ACTION_USER_MANAGEMENT = "user_management";

    /**
     * Returns {@code true} if this {@code Session} has permission to
     * perform the specified actions at the specified {@code absPath} and
     * {@code false} otherwise.
     * <p>
     * The {@code actions} parameter is a list of action strings. Apart
     * from the actions defined on {@link Session}, this variant also allows
     * to specify the following additional actions to provide better permission
     * discovery:
     * <ul>
     * <li> {@link
     * #ACTION_ADD_PROPERTY {@code add_property}}: If {@code hasPermission(path,
     * "add_property")} returns {@code true}, then this {@code Session} has
     * permission to add a new property at {@code path}.</li>
     * <li> {@link #ACTION_MODIFY_PROPERTY {@code modify_property}}: If
     * {@code hasPermission(path, "modify_property")} returns
     * {@code true}, then this {@code Session} has permission to change
     * a property at {@code path}. </li>
     * <li> {@link
     * #ACTION_REMOVE_PROPERTY {@code remove_property}}: If {@code hasPermission(path,
     * "remove_property")} returns {@code true}, then this {@code Session} has
     * permission to remove a property at {@code path}. </li>
     * <li> {@link #ACTION_REMOVE_NODE {@code remove_node}}: If
     * {@code hasPermission(path, "remove_node")} returns {@code true}, then
     * this {@code Session} has permission to remove a node at {@code path}. </li>
     * <li> {@link #ACTION_NODE_TYPE_MANAGEMENT {@code node_type_management}}: If
     * {@code hasPermission(path, "node_type_management")} returns {@code true}, then
     * this {@code Session} has permission to explicitly set or change the node type
     * information associated with a node at {@code path}. </li>
     * <li> {@link #ACTION_VERSIONING {@code versioning}}: If
     * {@code hasPermission(path, "versioning")} returns {@code true}, then
     * this {@code Session} has permission to perform version related operations
     * on a node at {@code path}. </li>
     * <li> {@link #ACTION_LOCKING {@code locking}}: If
     * {@code hasPermission(path, "locking")} returns {@code true}, then
     * this {@code Session} has permission to lock and unlock a node at {@code path}. </li>
     * <li> {@link #ACTION_READ_ACCESS_CONTROL {@code read_access_control}}: If
     * {@code hasPermission(path, "read_access_control")} returns {@code true}, then
     * this {@code Session} has permission to read access control content stored
     * at an item at {@code path}. </li>
     * <li> {@link #ACTION_MODIFY_ACCESS_CONTROL {@code modify_access_control}}: If
     * {@code hasPermission(path, "modify_access_control")} returns {@code true}, then
     * this {@code Session} has permission to modify access control content
     * at an item at {@code path}. </li>
     * <li> {@link #ACTION_USER_MANAGEMENT {@code user_management}}: If
     * {@code hasPermission(path, "user_management")} returns {@code true}, then
     * this {@code Session} has permission to perform user management operations
     * at an item at {@code path}. </li>
     * </ul>
     *
     * When more than one action is specified, this method will only return
     * {@code true} if this {@code Session} has permission to perform <i>all</i>
     * of the listed actions at the specified path.
     * <p>
     * The information returned through this method will only reflect the permission
     * status (both JCR defined and implementation-specific) and not
     * other restrictions that may exist, such as node type or other
     * implementation enforced constraints. For example, even though
     * {@code hasPermission} may indicate that a particular {@code Session} may
     * add a property at {@code /A/B/C}, the node type of the node at {@code /A/B}
     * may prevent the addition of a property called {@code C}.
     *
     * @param absPath an absolute path.
     * @param actions one or several actions.
     * @return {@code true} if this {@code Session} has permission to
     *         perform the specified actions at the specified
     *         {@code absPath}.
     * @throws RepositoryException if an error occurs.
     * @see Session#hasPermission(String, String)
     */
    boolean hasPermission(@NotNull String absPath, @NotNull String... actions) throws RepositoryException;

    /**
     * Returns the <code>PrincipalManager</code> for the current <code>Session</code>.
     *
     * @return the <code>PrincipalManager</code> associated with this <code>Session</code>.
     * @throws AccessDeniedException If the session lacks privileges to access
     * the principal manager or principals in general.
     * @throws UnsupportedRepositoryOperationException If principal management
     * is not supported.
     * @throws RepositoryException If another error occurs.
     * @see PrincipalManager
     */
    PrincipalManager getPrincipalManager() throws AccessDeniedException, UnsupportedRepositoryOperationException, RepositoryException;

    /**
     * Returns the <code>UserManager</code> for the current <code>Session</code>.
     *
     * @return the <code>UserManager</code> associated with this <code>Session</code>.
     * @throws javax.jcr.AccessDeniedException If this session is not allowed to
     * to access user data.
     * @throws UnsupportedRepositoryOperationException If user management is
     * not supported.
     * @throws javax.jcr.RepositoryException If another error occurs.
     * @see UserManager
     */
    UserManager getUserManager() throws AccessDeniedException, UnsupportedRepositoryOperationException, RepositoryException;

    /**
     * Returns the node at the specified absolute path in the workspace. If no
     * such node exists, then it returns the property at the specified path.
     * If no such property exists, then it return {@code null}.
     *
     * @param absPath An absolute path.
     * @return the specified {@code Item} or {@code null}.
     * @throws RepositoryException if another error occurs.
     * @since 2.11.1
     */
    Item getItemOrNull(final String absPath) throws RepositoryException;

    /**
     * Returns the property at the specified absolute path in the workspace or
     * {@code null} if no such node exists.
     *
     * @param absPath An absolute path.
     * @return the specified {@code Property} or {@code null}.
     * @throws RepositoryException if another error occurs.
     * @since 2.11.1
     */
    Property getPropertyOrNull(final String absPath) throws RepositoryException;

    /**
     * Returns the node at the specified absolute path in the workspace or
     * {@code null} if no such node exists.
     *
     * @param absPath An absolute path.
     * @return the specified {@code Node} or {@code null}.
     * @throws RepositoryException If another error occurs.
     * @since 2.11.1
     */
    Node getNodeOrNull(final String absPath) throws RepositoryException;

    /**
     * Returns the parent of the given {@code Item} or {@code null} if no parent exists (either because the given {@code Item} 
     * represents the root node or the current session does not have sufficient access to retrieve the parent).
     * 
     * @param item An {@code Item} that has been obtained by the current session.
     * @return The parent node of the given {@code Item} or {@code null}.
     * @throws RepositoryException If another error occurs.
     * @see Item#getParent() 
     * @since 1.42
     */
    @Nullable 
    default Node getParentOrNull(@NotNull Item item) throws RepositoryException {
        if (item instanceof JackrabbitNode) {
            return ((JackrabbitNode) item).getParentOrNull();
        }
        try {
            return item.getParent();
        } catch (ItemNotFoundException | AccessDeniedException e) {
            return null;
        }
    }
}
