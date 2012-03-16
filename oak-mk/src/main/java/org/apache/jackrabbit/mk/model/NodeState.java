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
package org.apache.jackrabbit.mk.model;

/**
 * A content tree consists of nodes and properties, each of which
 * evolves through different states during its lifecycle. This interface
 * represents a specific, immutable state of a node in a content tree.
 * A node consists of an unordered set of name -&gt; item mappings, where
 * each item is either a property or a child node.
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
 * In addition to being immutable, a specific state instance guaranteed to
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
 * Since this interface exposes no higher level constructs like access
 * controls, locking, node types or even path parsing, there's no way
 * for content access to fail because of such concerns. Such functionality
 * and related checked exceptions or other control flow constructs should
 * be implemented on a higher level above this interface.
 *
 * <h2>Decoration and virtual content</h2>
 * <p>
 * Not all content exposed by this interface needs to be backed by actual
 * persisted data. An implementation may want to provide derived data,
 * like for example the aggregate size of the entire subtree as an
 * extra virtual property. A virtualization, sharding or caching layer
 * could provide a composite view over multiple underlying content trees.
 * Or a basic access control layer could decide to hide certain content
 * based on specific rules. All such features need to be implemented
 * according to the API contract of this interface. A separate higher level
 * interface needs to be used if an implementation can't for example
 * guarantee immutability of exposed content as discussed above.
 *
 * <h2>Equality and hash codes</h2>
 * <p>
 * Two node states are considered equal if and only if their properties and
 * child nodes match, regardless of ordering. The
 * {@link Object#equals(Object)} method needs to be implemented so that it
 * complies with this definition. And while node states are not meant for
 * use as hash keys, the {@link Object#hashCode()} method should still be
 * implemented according to this equality contract.
 */
public interface NodeState {

    /**
     * Returns the named property. The name is an opaque string and
     * is not parsed or otherwise interpreted by this method.
     * <p>
     * The namespace of properties and child nodes is shared, so if
     * this method returns a non-<code>null</code> value for a given
     * name, then {@link #getChildNode(String)} is guaranteed to return
     * <code>null</code> for the same name.
     *
     * @param name name of the property to return
     * @return named property, or <code>null</code> if not found
     */
    PropertyState getProperty(String name);

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
    Iterable<? extends PropertyState> getProperties();

    /**
     * Returns the named child node. The name is an opaque string and
     * is not parsed or otherwise interpreted by this method.
     * <p>
     * The namespace of properties and child nodes is shared, so if
     * this method returns a non-<code>null</code> value for a given
     * name, then {@link #getProperty(String)} is guaranteed to return
     * <code>null</code> for the same name.
     *
     * @param name name of the child node to return
     * @return named child node, or <code>null</code> if not found
     */
    NodeState getChildNode(String name);

    /**
     * Returns the number of child nodes of this node.
     *
     * @return number of child nodes
     */
    long getChildNodeCount();

    /**
     * Returns an iterable of the child node entries starting from the
     * given offset. Multiple iterations are guaranteed to return the
     * child nodes in the same order, but the specific order used is
     * implementation-dependent and may change across different states
     * of the same node. An empty iterable is returned if the given
     * offset is greater than the offset of the last child node entry.
     *
     * @param offset zero-based offset of the first entry to return
     * @param count  maximum number of entries to return,
     *               or -1 for all remaining entries
     * @return requested child node entries in some stable order
     */
    Iterable<? extends ChildNodeEntry> getChildNodeEntries(long offset, int count);

}
