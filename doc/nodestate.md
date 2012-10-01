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
  * The `getBuilder` method for building modified states
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

The last two methods, `getBuilder` and `compareAgainstBaseState`, are
covered in the next two sections. See also the `NodeState` javadocs for
more details about this interface and all its methods.

## Building new node states

Since node states are immutable, a separate builder interface,
`NodeBuilder`, is used to construct new, modified node states. Calling
the `getBuilder` method on a node state returns such a builder for
modifying that node and the subtree below it.

A node builder can be thought of as a _mutable_ version of a node state.
In addition to property and child node access methods like the ones that
are already present in the `NodeState` interface, the `NodeBuilder`
interface contains the following key methods:

  * The `setProperty` and `removeProperty` methods for modifying properties
  * The `removeNode` method for removing a subtree
  * The `setNode` method for adding or replacing a subtree
  * The `getChildBuilder` method for creating or modifying a subtree with
    a connected child builder
  * The `getNodeState` method for getting a frozen snapshot of the modified
    content tree

The concept of _connected builders_ is designed to make it easy to manage
complex content changes. Since individual node states are always immutable,
modifying a particular node at a path like `/foo/bar` using the `setNode`
method would require the following overly verbose code:

    NodeState root = …;
    NodeState foo = root.getChildNode("foo")
    NodeState bar = foo.getChildNode("bar");
    NodeBuilder barBuilder = bar.getBuilder();
    barBuilder.setProperty("test", …);
    NodeBuilder fooBuilder = foo.getBuilder();
    fooBuilder.setNode("bar", barBuilder.getNodeState());
    NodeBuilder rootBuilder = root.getBuilder();
    rootBuilder.setNode("foo", fooBuilder.getNodeState());
    root = rootBuilder.getNodeState();

The complexity here is caused by the need to explicitly construct and
re-connect each modified node state along the path from the root to the
modified content in `/foo/bar`. This is because each `NodeBuilder` instance
created by the `getBuilder` method is independent and can only be used to
affect other builders in the manner shown above. In contrast the
`getChildBuilder` method returns a builder instance that is "connected" to
the parent builder in a way that any changes recorded in the child builder
will automatically show up also in the node states created by the parent
builder. With connected builders the above code can be simplified to:

    NodeState root = …;
    NodeBuilder rootBuilder = root.getBuilder();
    rootBuilder
        .getChildBuilder("foo")
        .getChildBuilder("bar")
        .setProperty("test", …);
    root = rootBuilder.getNodeState();

Typically the only case where the `setNode` method is preferable over
`getChildBuilder` is when moving or copying subtrees from one location
to another. For example, the following code copies the `/orig` subtree
to `/copy`:

    NodeState root = …;
    NodeBuilder rootBuilder = root.getBuilder();
    rootBuilder.setNode("copy", root.getChildNode("orig"));
    root = rootBuilder.getNodeState();

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

    ContentRepository contentRepository = new Oak()
        .with(new DenyContentWithName("bar"))
        .createContentRepository();

    Repository repository = new RepositoryImpl(contentRepository);
    Session session = repository.login();
    Node root = session.getRootNode();
    root.setProperty("foo", "test");
    session.save();
    root.setProperty("bar", "test");
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

