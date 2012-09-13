# Understanding the node state model

This article describes the node state model that is the core design
abstraction inside the oak-core component. Understanding the node state
model is essential to working with Oak internals and to building custom
Oak extensions.

## Background

Oak organizes all content in a large tree hierarcy that consists of nodes
and properties. Each snapshot or revision of this content tree is immutable,
and changes to the tree are expressed as a sequence of new revisions. The
MicroKernel of an Oak repository is responsible for managing the content
tree and its revisions.

The JSON-based MicroKernel API works well as a part of a remote protocol
but is cumbersome to use directly in oak-core. There are also many cases
where transient or virtual content that doesn't (yet) exist in the
MicroKernel needs to be managed by Oak. The node state model as expressed
in the NodeState interface in oak-core is designed for these purposes. It
provides a unified low-level abstraction for managing all tree content and
lays the foundation for the higher-level Oak API that's visible to clients.

## The state of a node

A _node_ in Oak is an unordered collection of named properties and child
nodes. As the content tree evolves through a sequence of revisions, a node
in it will go through a series of different states. A _node state_  then is
an _immutable_ snapshot of a specific state of a node and the subtree beneath
it.

To avoid making a special case of the root node and therefore to make it
easy to write algorithms that can recursively process each subtree as a
standalone content tree, a node state is _unnamed_ and does not contain
information about it's location within a larger content tree. Instead each
property and child node state is uniquely named within a parent node state.
An algorithm that needs to know the path of a node can construct it from
the encountered names as it descends the tree structure.

Since node states are immutable, they are also easy to keep _thread-safe_.
Implementations that use mutable data structures like caches or otherwise
aren't thread-safe by default, are expected to use other mechanisms like
synchronization to ensure thread-safety.

## The NodeState interface

The above design principles are reflected in the `NodeState` interface
in the `org.apache.jackrabbit.oak.spi.state` package of oak-core. The
interface consits of three sets of methods:

  * Methods for accessing properties
  * Methods for accessing child nodes
  * The `compareAgainstBaseState` method for comparing states

You can request a property or a child node by name, get the number of
properties or child nodes, or iterate through all of them. Even though
properties and child nodes are accessed through separate methods, they
share the same namespace so a given name can either refer to a property
or a child node, but not to both at the same time.

Iteration order of properties and child nodes is _unspecified but stable_,
so that re-iterating through the items of a _specific NodeState instance_
will return the items in the same order as before, but the specific ordering
is not defined nor does it necessarily remain the same across different
instances.

The `compareAgainstBaseState` method takes another NodeState instance and
a `NodeStateDiff` object, compares the two node states, and reports all
differences by invoking appropriate methods on the given diff handler object.

See the `NodeState` javadocs for full details of how the interface works.

## Comparing node states

TODO

## Building new node states

TODO

## The commit hook mechanism

TODO

## Commit validation

TODO

## Commit modification

TODO

