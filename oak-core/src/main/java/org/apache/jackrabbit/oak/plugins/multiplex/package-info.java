/**
 * <h1>Multiplexing support</h1>
 *
 * <p>This package contains support classes for implementing a multiplexed persistence at the {@linkplain org.apache.jackrabbit.oak.spi.state.NodeStore} level.</p>
 *
 * <h2>Design goals</h2>
 * <p>
 * <ol>
 *   <li>Transparency of the multiplexing setup. Neither the NodeStores nor the code using a multiplexed
 *       NodeStore should be aware of the specific implementation being used.</li>
 *   <li>Persistence-agnosticity. The multiplexing support should be applicable to any conformant
 *       NodeStore implementation.</li>
 *   <li>Negligible performance impact. Multiplexing should not add a significat performance overhead.</li>
 * </ol>
 * </p>
 *
 * <h2>Implementation</h2>
 *
 * <p>The main entry point is the {@link org.apache.jackrabbit.oak.plugins.multiplex.MultiplexingNodeStore},
 * which wraps one or more NodeStore instances. Also of interest are the {@link org.apache.jackrabbit.oak.plugins.multiplex.MultiplexingNodeState} and {@link org.apache.jackrabbit.oak.plugins.multiplex.MultiplexingNodeBuilder}.</p>
 *
 * <p> These classes maintain internal mappings of the 'native' objects. For instance, if the
 * multiplexing NodeStore holds two MemoryNodeStore instances, then a call to {@linkplain org.apache.jackrabbit.oak.spi.state.NodeStore#getRoot()}
 * will return a multiplexing NodeState backed by two MemoryNodeState instances. Similarly, a call to {@linkplain org.apache.jackrabbit.oak.spi.state.NodeState#builder()} will return a multiplexing
 * NodeBuilder backed by two MemoryNodeState instances.</p>
 *
 * <p>Using this approach allows us to always keep related NodeStore, NodeState and NodeBuilder
 * instances isolated from other instances.</p>
 *
 * <h2>Open items</h2>
 *
 * <p>1. Brute-force support for oak:mount nodes.</p>
 *
 *  <p>The {@link org.apache.jackrabbit.oak.spi.mount.Mount#getPathFragmentName()} method defines
 *  a name pattern that can be used by mounted stores to contribute to a patch which is not
 *  owned by them. For instance, a mount named <em>apps</em> which owns <tt>/libs,/apps</tt>
 *  can own another subtree anywhere in the repository given that a node named <tt>:oak-mount-apps</tt>
 *  is found.</p>
 *
 *  <p>The current implementation naively queries all stores whenever the child node list is prepared.
 *  This is obviously correct but may be slow.</p>
 *  {@link org.apache.jackrabbit.oak.plugins.multiplex.MultiplexingContext#getContributingStores(java.lang.String, com.google.common.base.Function)}</p>
 */
package org.apache.jackrabbit.oak.plugins.multiplex;

