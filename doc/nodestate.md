<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  -->

# Understanding the node state model

This article describes the node state model that is the core design
abstraction inside the oak-core component. Understanding the node state
model is essential to working with Oak internals and to building custom
Oak extensions.

## Background

Oak organizes all content in a large tree hierarchy that consists of nodes
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

As an example, the following diagram shows two revisions of a content tree,
the first revision consists of five nodes, and in the second revision a
sixth node is added in one of the subtrees. Note how unmodified subtrees
can be shared across revisions, while only the modified nodes and their
ancestors up to the root (shown in yellow) need to be updated to reflect
the change. This way both revisions remain readable at all times without
the implementation having to make a separate copy of the entire repository
for each revision.

![two revisions of a content tree](nodestate-r1.png?raw=true)

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
interface consists of three sets of methods:

  * Methods for accessing properties
  * Methods for accessing child nodes
  * The `exists` method for checking whether the node exists or is accessible
  * The `builder` method for building modified states
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

The last three methods, `exists`, `builder` and `compareAgainstBaseState`,
are covered in the next sections. See also the `NodeState` javadocs for
more details about this interface and all its methods.

## Existence and iterability of node states

The `exists` method makes it possible to always traverse any path regardless
of whether the named content exists or not. The `getChildNode` method returns
a child `NodeState` instance for any given name, and the caller is expected
to use the `exists` method to check whether the named node actually does exist.
The purpose of this feature is to allow paths like `/foo/bar` to be traversed
even if the current user only has read access to the `bar` node but not its
parent `foo`. As a consequence it's even possible to access content like a
fictional `/bar` subtree that doesn't exist at all. A piece of code for
accessing such content could look like this:

```java
NodeState root = ...;

NodeState foo = root.getChildNode("foo");
assert !foo.exists();
NodeState bar = foo.getChildNode("bar");
assert bar.exists();

NodeState baz = root.getChildNode("baz");
assert !baz.exists();
```

The following diagram illustrates such a content tree, both as the raw
content that simply exists and as an access controlled view of that tree:

![content tree with and without access control](nodestate-r2.png?raw=true)

If a node is missing, i.e. its `exists` method returns `false` and thus it
either does not exist at all or is read-protected, one can't list any of
its properties or child nodes. And on the other hand, a non-readable node or
property will only show up when listing the child nodes or properties of its
parent. In other words, a node or a property is _iterable_ only if both it
and its parent are readable. For example, attempts to list properties or
child nodes of the node `foo` will show up empty:

```java
assert !foo.getProperties().iterator().hasNext();
assert !foo.getChildNodeEntries().iterator().hasNext();
```

Note that without some external knowledge about the accessibility of a child
node like `bar` there's no way for a piece of code to distinguish between
the existing but non-readable node `foo` and the non-existing node `baz`.

## Building new node states

Since node states are immutable, a separate builder interface,
`NodeBuilder`, is used to construct new, modified node states. Calling
the `builder` method on a node state returns such a builder for
modifying that node and the subtree below it.

A node builder can be thought of as a _mutable_ version of a node state.
In addition to property and child node access methods like the ones that
are already present in the `NodeState` interface, the `NodeBuilder`
interface contains the following key methods:

  * The `setProperty` and `removeProperty` methods for modifying properties
  * The `getChild` method for accessing or modifying an existing subtree
  * The `addNode`, `setNode` and `removeNode` methods for adding, replacing
    or removing a subtree
  * The `exists` method for checking whether the node represented by
    a builder exists or is accessible
  * The `getNodeState` method for getting a frozen snapshot of the modified
    content tree

All the builders acquired from the same root builder instance are connected
so that changes made through one instance automatically become visible in the
other builders. For example:

```java
NodeBuilder rootBuilder = root.builder();
NodeBuilder fooBuilder = rootBuilder.getChild("foo");
NodeBuilder barBuilder = fooBuilder.getChild("bar");

assert !barBuilder.getBoolean("x");
fooBuilder.getChild("bar").setProperty("x", Boolean.TRUE);
assert barBuilder.getBoolean("x");

assert barBuilder.exists();
fooBuilder.removeNode("bar");
assert !barBuilder.exists();
```

The `getNodeState` method returns a frozen, immutable snapshot of the current
state of the builder. Providing such a snapshot can be somewhat expensive
especially if there are many changes in the builder, so the method should
generally only be used as the last step after all intended changes have
been made. Meanwhile the accessors in the `NodeBuilder` interface can be
used to provide efficient read access to the current state of the tree
being modified.

The node states constructed by a builder often retain an internal reference
to the base state used by the builder. This allows common node state
comparisons to perform really well as described in the next section.

## Comparing node states

As a node evolves through a sequence of states, it's often important to
be able to tell what has changed between two states of the node. This
functionality is available through the `compareAgainstBaseState` method.
The method takes two arguments:

  * A _base state_ for the comparison. The comparison will report all
    changes necessary for moving from the given base state to the node
    state on which the comparison method is invoked.
  * A `NodeStateDiff` instance to which all detected changes are reported.
    The diff interface contains callback methods for reporting added,
    modified or removed properties or child nodes.

The comparison method can actually be used to compare any two nodes, but the
implementations of the method are typically heavily optimized for the case
when the given base state actually is an earlier version of the same node.
In practice this is by far the most common scenario for node state comparisons,
and can typically be executed in `O(d)` time where `d` is the number of
changes between the two states. The fallback strategy for comparing two
completely unrelated node states can be much more expensive.

An important detail of the `NodeStateDiff` mechanism is the `childNodeChanged`
method that will get called if there are _any_ changes in the subtree starting
at the named child node. The comparison method should thus be able to
efficiently detect differences at any depth below the given nodes. On the
other hand the `childNodeChanged` method is called only for the direct child
node, and the diff implementation should explicitly recurse down the tree
if it wants to know what exactly did change under that subtree. The code
for such recursion typically looks something like this:

    public void childNodeChanged(
            String name, NodeState before, NodeState after) {
        after.compareAgainstBaseState(before, ...);
    }

## The commit hook mechanism

TODO

## Commit validation

TODO

TODO: Basic validator class

    class DenyContentWithName extends DefaultValidator {

        private final String name;

        public DenyContentWithName(String name) {
            this.name = name;
        }

        @Override
        public void propertyAdded(PropertyState after)
                throws CommitFailedException {
            if (name.equals(after.getName())) {
                throw new CommitFailedException(
                        "Properties named " + name + " are not allowed");
            }
        }


    }

TODO: Example of how the validator works

    Repository repository = new Jcr()
        .with(new DenyContentWithName("bar"))
        .createRepository();

    Session session = repository.login();
    Node root = session.getRootNode();
    root.setProperty("foo", "abc");
    session.save();
    root.setProperty("bar", "def");
    session.save(); // will throw an exception

TODO: Extended example that also works below root and covers also node names

    class DenyContentWithName extends DefaultValidator {

        private final String name;

        public DenyContentWithName(String name) {
            this.name = name;
        }

        private void testName(String addedName) throws CommitFailedException {
            if (name.equals(addedName)) {
                throw new CommitFailedException(
                        "Content named " + name + " is not allowed");
            }
        }

        @Override
        public void propertyAdded(PropertyState after)
                throws CommitFailedException {
            testName(after.getName());
        }

        @Override
        public Validator childNodeAdded(String name, NodeState after)
                throws CommitFailedException {
            testName(name);
            return this;
        }

        @Override
        public Validator childNodeChanged(
                String name, NodeState before, NodeState after)
                throws CommitFailedException {
            return this;
        }

    }

## Commit modification

TODO

TODO: Basic commit hook example

    class RenameContentHook implements CommitHook {

        private final String name;

        private final String rename;

        public RenameContentHook(String name, String rename) {
            this.name = name;
            this.rename = rename;
        }

        @Override @Nonnull
        public NodeState processCommit(NodeState before, NodeState after)
                throws CommitFailedException {
            PropertyState property = after.getProperty(name);
            if (property != null) {
                NodeBuilder builder = after.builder();
                builder.removeProperty(name);
                if (property.isArray()) {
                    builder.setProperty(rename, property.getValues());
                } else {
                    builder.setProperty(rename, property.getValue());
                }
                return builder.getNodeState();
            }
            return after;
        }

    }

TODO: Using the commit hook to avoid the exception from a validator

    Repository repository = new Jcr()
        .with(new RenameContentHook("bar", "foo"))
        .with(new DenyContentWithName("bar"))
        .createRepository();

    Session session = repository.login();
    Node root = session.getRootNode();
    root.setProperty("foo", "abc");
    session.save();
    root.setProperty("bar", "def");
    session.save(); // will not throw an exception!
    System.out.println(root.getProperty("foo").getString()); // Prints "def"!

