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
 *   <li>Neglibigle performance impact. Multiplexing should not add a significat performance overhead.</li> 
 * </ol>
 * </p>
 * 
 * <h2>Implementation</h2>
 * 
 * <p>The main entry point is the {@link org.apache.jackrabbit.oak.plugins.multiplex.MultiplexingNodeStore},
 * which wraps one or more NodeStore instances. Also of interest are the {@link org.apache.jackrabbit.oak.plugins.multiplex.MultiplexingNodeState} and {@link org.apache.jackrabbit.oak.plugins.multiplex.MultiplexingNodeBuilder}.</p>
 * 
 * <p> These classes maintain internal mappings of the 'native' objects.For instance, if the 
 * multiplexing NodeStore holds two MemoryNodeStore instances, then a call to {@linkplain org.apache.jackrabbit.oak.spi.state.NodeStore#getRoot()} 
 * will return a multiplexing NodeState backed by two MemoryNodeState instances. Similarly, a call to {@linkplain org.apache.jackrabbit.oak.spi.state.NodeState#builder()} will return a multiplexing
 * NodeBuilder backed by two MemoryNodeState instances.</p>
 * 
 * <p>Using this approach allows us to always keep related NodeStore, NodeState and NodeBuilder
 * instances isolated from other instances.</p>
 * 
 * <h2>Open items</h2>
 * 
 * <p>1. Need to support clients which expect that the NodeState is an instance or subclass
 * of MemoryNodeState</p>
 * 
 * <p>Currently there are some initialisers in oak that unconditionally pass a NodeState
 * to a MemoryNodeStore:
 * 
 * <ol>
 *   <li>{@link org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent}</li>
 *   <li>{@link org.apache.jackrabbit.oak.security.privilege.PrivilegeInitializer}</li>
 *   <li>{@link org.apache.jackrabbit.oak.security.user.UserInitializer}</li>
 * </ol>
 * 
 * Since the multiplexing classes don't inherit from the memory-based classes this fails at 
 * runtime. The compromise that was reached was to add a new {@link org.apache.jackrabbit.oak.spi.state.HasNativeNodeBuilder} interface, implemented by the 
 * multiplexing NodeStore, which allows exposing the native node builder, which is a MemoryNodeBuilder
 * instance or subclass. This is not ideal though, as it forces (some) users to know about the
 * multiplexing specifics.
 * </p>
 * 
 * <p>2. Brute-force support for oak:mount nodes.</p>
 *  
 *  <p>The {@link org.apache.jackrabbit.oak.spi.mount.Mount#getPathFragmentName()} method defines
 *  a name pattern that can be used by mounted stores to contribute to a patch which is not
 *  owned by them. For instance, a mount named <em>apps</em> which owns <tt>/libs,/apps</tt>
 *  can own another subtree anywhere in the repository given that a node named <tt>:oak-mount-apps</tt>
 *  is found.</p>
 *  
 *  <p>The current implementation naively queries all stores whenever the child node list is prepared.
 *  This is obviously correct but painfully slow.</p> 
 */
package org.apache.jackrabbit.oak.plugins.multiplex;

