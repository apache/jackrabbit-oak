/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.state;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;

import com.google.common.base.Predicate;

/**
 * A node in a content tree consists of child nodes and properties, each
 * of which evolves through different states during its lifecycle. This
 * interface represents a specific, immutable state of a node.
 * <p>
 * The state of a node consists of named properties and child nodes. Names
 * are non-empty strings that never contain the forward slash character, "/".
 * Implementations may place additional restrictions on possible name strings.
 * The properties and child nodes are unordered, and no two properties or
 * two child nodes may have the same name. An implementation may additionally
 * restrict a property and a child node from having the same name.
 * <p>
 * Depending on context, a NodeState instance can be interpreted as
 * representing the state of just that node, of the subtree starting at
 * that node, or of an entire tree in case it's a root node.
 * <p>
 * The crucial difference between this interface and the similarly named
 * class in Jackrabbit 2.x is that this interface represents a specific,
 * immutable state of a node, whereas the Jackrabbit 2.x class represented
 * the <em>current</em> state of a node.
 *
 * <h2>Immutability and thread-safety</h2>
 * <p>
 * As mentioned above, all node and property states are always immutable.
 * Thus repeating a method call is always guaranteed to produce the same
 * result as before unless some internal error occurs (see below). This
 * immutability only applies to a specific state instance. Different states
 * of a node can obviously be different, and in some cases even different
 * instances of the same state may behave slightly differently. For example
 * due to performance optimization or other similar changes the iteration
 * order of properties or child nodes may be different for two instances
 * of the same state.
 * <p>
 * In addition to being immutable, a specific state instance is guaranteed to
 * be fully thread-safe. Possible caching or other internal changes need to
 * be properly synchronized so that any number of concurrent clients can
 * safely access a state instance.
 *
 * <h2>Persistence and error-handling</h2>
 * <p>
 * A node state can be (and often is) backed by local files or network
 * resources. All IO operations or related concerns like caching should be
 * handled transparently below this interface. Potential IO problems and
 * recovery attempts like retrying a timed-out network access need to be
 * handled below this interface, and only hard errors should be thrown up
 * as {@link RuntimeException unchecked exceptions} that higher level code
 * is not expected to be able to recover from.
 * <p>
 * Since this interface exposes no higher level constructs like locking,
 * node types or even path parsing, there's no way for content access to
 * fail because of such concerns. Such functionality and related checked
 * exceptions or other control flow constructs should be implemented on
 * a higher level above this interface. On the other hand read access
 * controls <em>can</em> be implemented below this interface, in which
 * case some content that would otherwise be accessible might not show
 * up through such an implementation.
 *
 * <h2>Existence and iterability of node states</h2>
 * <p>
 * The {@link #getChildNode(String)} method is special in that it
 * <em>never</em> returns a {@code null} value, even if the named child
 * node does not exist. Instead a client should use the {@link #exists()}
 * method on the returned child state to check whether that node exists.
 * The purpose of this separation of concerns is to allow an implementation
 * to lazily load content only when it's actually read instead of just
 * traversed. It also simplifies client code by avoiding the need for many
 * {@code null} checks when traversing paths.
 * <p>
 * The <em>iterability</em> of a node is a related concept to the
 * above-mentioned existence. A node state is <em>iterable</em> if it
 * is included in the return values of the {@link #getChildNodeCount(long)},
 * {@link #getChildNodeNames()} and {@link #getChildNodeEntries()} methods.
 * An iterable node is guaranteed to exist, though not all existing nodes
 * are necessarily iterable.
 * <p>
 * Furthermore, a non-existing node is guaranteed to contain no properties
 * or iterable child nodes. It can, however contain non-iterable children.
 * Such scenarios are typically the result of access control restrictions.
 *
 * <h2>Decoration and virtual content</h2>
 * <p>
 * Not all content exposed by this interface needs to be backed by actual
 * persisted data. An implementation may want to provide derived data,
 * like for example the aggregate size of the entire subtree as an
 * extra virtual property. A virtualization, sharding or caching layer
 * could provide a composite view over multiple underlying trees.
 * Or a an access control layer could decide to hide certain content
 * based on specific rules. All such features need to be implemented
 * according to the API contract of this interface. A separate higher level
 * interface needs to be used if an implementation can't for example
 * guarantee immutability of exposed content as discussed above.
 *
 * <h2>Equality and hash codes</h2>
 * <p>
 * Two node states are considered equal if and only if their existence,
 * properties and iterable child nodes match, regardless of ordering. The
 * {@link Object#equals(Object)} method needs to be implemented so that it
 * complies with this definition. And while node states are not meant for
 * use as hash keys, the {@link Object#hashCode()} method should still be
 * implemented according to this equality contract.
 */
public interface NodeState {

    /**
     * Checks whether this node exists. See the above discussion about
     * the existence of node states.
     *
     * @return {@code true} if this node exists, {@code false} if not
     */
    boolean exists();

    /**
     * Checks whether the named property exists. The implementation is
     * equivalent to {@code getProperty(name) != null}, but may be optimized
     * to avoid having to load the property value.
     *
     * @param name property name
     * @return {@code true} if the named property exists,
     *         {@code false} otherwise
     */
    boolean hasProperty(@Nonnull String name);

    /**
     * Returns the named property, or {@code null} if no such property exists.
     *
     * @param name name of the property to return
     * @return named property, or {@code null} if not found
     */
    @CheckForNull
    PropertyState getProperty(@Nonnull String name);

    /**
     * Returns the boolean value of the named property. The implementation
     * is equivalent to the following code, but may be optimized.
     * <pre>
     * {@code
     * PropertyState property = state.getProperty(name);
     * return property != null
     *     && property.getType() == Type.BOOLEAN
     *     && property.getValue(Type.BOOLEAN);
     * }
     * </pre>
     *
     * @param name property name
     * @return boolean value of the named property, or {@code false}
     */
    boolean getBoolean(@Nonnull String name);

    /**
     * Returns the long value of the named property. The implementation
     * is equivalent to the following code, but may be optimized.
     * <pre>
     * {@code
     * PropertyState property = state.getProperty(name);
     * if (property != null && property.getType() == Type.LONG) {
     *     return property.getValue(Type.LONG);
     * } else {
     *     return 0;
     * }
     * }
     * </pre>
     *
     * @param name property name
     * @return long value of the named property, or zero
     */
    long getLong(String name);


    /**
     * Returns the string value of the named property. The implementation
     * is equivalent to the following code, but may be optimized.
     * <pre>
     * {@code
     * PropertyState property = state.getProperty(name);
     * if (property != null && property.getType() == Type.STRING) {
     *     return property.getValue(Type.STRING);
     * } else {
     *     return null;
     * }
     * }
     * </pre>
     *
     * @param name property name
     * @return string value of the named property, or {@code null}
     */
    @CheckForNull
    String getString(String name);

    /**
     * Returns the string values of the named property. The implementation
     * is equivalent to the following code, but may be optimized.
     * <pre>
     * {@code
     * PropertyState property = state.getProperty(name);
     * if (property != null && property.getType() == Type.STRINGS) {
     *     return property.getValue(Type.STRINGS);
     * } else {
     *     return Collections.emptyList();
     * }
     * }
     * </pre>
     *
     * @param name property name
     * @return string values of the named property, or an empty collection
     */
    @Nonnull
    Iterable<String> getStrings(@Nonnull String name);

    /**
     * Returns the name value of the named property. The implementation
     * is equivalent to the following code, but may be optimized.
     * <pre>
     * {@code
     * PropertyState property = state.getProperty(name);
     * if (property != null && property.getType() == Type.NAME) {
     *     return property.getValue(Type.NAME);
     * } else {
     *     return null;
     * }
     * }
     * </pre>
     *
     * @param name property name
     * @return name value of the named property, or {@code null}
     */
    @CheckForNull
    String getName(@Nonnull String name);

    /**
     * Returns the name values of the named property. The implementation
     * is equivalent to the following code, but may be optimized.
     * <pre>
     * {@code
     * PropertyState property = state.getProperty(name);
     * if (property != null && property.getType() == Type.NAMES) {
     *     return property.getValue(Type.NAMES);
     * } else {
     *     return Collections.emptyList();
     * }
     * }
     * </pre>
     *
     * @param name property name
     * @return name values of the named property, or an empty collection
     */
    @Nonnull
    Iterable<String> getNames(@Nonnull String name);

    /**
     * Returns the number of properties of this node.
     *
     * @return number of properties
     */
    long getPropertyCount();

    /**
     * Returns an iterable of the properties of this node. Multiple
     * iterations are guaranteed to return the properties in the same
     * order, but the specific order used is implementation-dependent
     * and may change across different states of the same node.
     *
     * @return properties in some stable order
     */
    @Nonnull
    Iterable<? extends PropertyState> getProperties();

    /**
     * Checks whether the named child node exists. The implementation
     * is equivalent to {@code getChildNode(name).exists()}, except that
     * passing an invalid name as argument will result in a {@code false}
     * return value instead of an {@link IllegalArgumentException}.
     *
     * @param name name of the child node
     * @return {@code true} if the named child node exists,
     *         {@code false} otherwise
     */
    boolean hasChildNode(@Nonnull String name);

    /**
     * Returns the named, possibly non-existent, child node. Use the
     * {@link #exists()} method on the returned child node to determine
     * whether the node exists or not.
     *
     * @param name name of the child node to return
     * @return named child node
     * @throws IllegalArgumentException if the given name string is is empty
     *                                  or contains a forward slash character
     */
    @Nonnull
    NodeState getChildNode(@Nonnull String name) throws IllegalArgumentException;

    /**
     * Returns the number of <em>iterable</em> child nodes of this node.
     * <p>
     * If an implementation knows the exact value, it returns it (even if
     * the value is higher than max). If the implementation does not know the
     * exact value, and the child node count is higher than max, it may return
     * Long.MAX_VALUE. The cost of the operation is at most O(max).
     * 
     * @param max the maximum number of entries to traverse
     * @return number of iterable child nodes
     */
    long getChildNodeCount(long max);

    /**
     * Returns the names of all <em>iterable</em> child nodes.
     *
     * @return child node names in some stable order
     */
    Iterable<String> getChildNodeNames();

    /**
     * Returns the <em>iterable</em> child node entries of this instance.
     * Multiple iterations are guaranteed to return the child nodes in
     * the same order, but the specific order used is implementation
     * dependent and may change across different states of the same node.
     * <p>
     * <i>Note on cost and performance:</i> while it is possible to iterate over
     * all child {@code NodeState}s with the two methods {@link
     * #getChildNodeNames()} and {@link #getChildNode(String)}, this method is
     * considered more efficient because an implementation can potentially
     * perform the retrieval of the name and {@code NodeState} in one call.
     * This results in O(n) vs. O(n log n) when iterating over the child node
     * names and then look up the {@code NodeState} by name.
     *
     * @return child node entries in some stable order
     */
    @Nonnull
    Iterable<? extends ChildNodeEntry> getChildNodeEntries();

    /**
     * Returns a builder for constructing a new node state based on
     * this state, i.e. starting with all the properties and child nodes
     * of this state.
     *
     * @since Oak 0.6
     * @return node builder based on this state
     */
    @Nonnull
    NodeBuilder builder();

    /**
     * Compares this node state against the given base state. Any differences
     * are reported by calling the relevant added/changed/deleted methods of
     * the given handler.
     * <p>
     * TODO: Define the behavior of this method with regards to
     * iterability/existence of child nodes.
     *
     * @param base base state
     * @param diff handler of node state differences
     * @return {@code true} if the full diff was performed, or
     *         {@code false} if it was aborted as requested by the handler
     *         (see the {@link NodeStateDiff} contract for more details)
     * @since 0ak 0.4, return value added in 0.7
     */
    boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff);

    /**
     * Predicate that checks the existence of NodeState instances.
     */
    Predicate<NodeState> EXISTS = new Predicate<NodeState>() {
        @Override
        public boolean apply(@Nullable NodeState input) {
            return input != null && input.exists();
        }
    };

}
