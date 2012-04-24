Oak Core
========

Oak API
-------

The API for accessing core Oak functionality is located in the
`org.apache.jackrabbit.oak.api` package and consists of the following
key interfaces:

  * ContentRepository
  * ContentSession
  * Root / Tree

The `ContentRepository` interface represents an entire Oak content repository.
The repository may local or remote, or a cluster of any size. These deployment
details are all hidden behind this interface.

Starting and stopping `ContentRepository` instances is the responsibility of
each particular deployment and not covered by these interfaces. Repository
clients should use a deployment-specific mechanism (JNDI, OSGi service, etc.)
to acquire references to `ContentRepository` instances.

All content in the repository is accessed through authenticated sessions
acquired through the `ContentRepository.login()` method. The method takes
explicit access credentials and other login details and, assuming the
credentials are valid, returns a `ContentSession` instance that encapsulates
this information. Session instances are `Closeable` and need to be closed
to release associated resources once no longer used. The recommended access
pattern is:

    ContentRepository repository = ...;
    ContentSession session = repository.login(...);
    try {
        ...; // Use the session
    } finally {
        session.close();
    }

All `ContentRepository` and `ContentSession` instances are thread-safe.

The authenticated `ContentSession` gives you properly authorized access to
the hierarchical content tree inside the repository through instances of the
`Root` and `Tree` interfaces. The `getCurrentRoot()` method returns a
snapshot of the current state of the content tree:

    ContentSession session = ...;
    Root root = session.getCurrentRoot();
    Tree tree = root.getTree();

The returned `Tree` instance belongs to the client and its state is only
modified in response to method calls made by the client. `Tree` instances
are *not* thread-safe for write access, so writing clients need to ensure
that they are not accessed concurrently from multiple threads. `Tree`
instances *are* however thread-safe for read access, so implementations
need to ensure that all reading clients see a coherent state.

Content trees are recursive data structures that consist of named properties 
and subtrees that share the same namespace, but are accessed through separate 
methods like outlined below:

    ContentTree tree = ...;
    for (PropertyState property : tree.getProperties()) {
        ...;
    }
    for (Tree subtree : tree.getSubtrees()) {
        ...;
    }

The repository content snapshot exposed by a `Tree` instance may become
invalid over time due to garbage collection of old content, at which point
an outdated snapshot will start throwing `IllegalStateExceptions` to
indicate that the snapshot is no longer available. To access more recent
content, a client should either call `getCurrentRoot()` to acquire a fresh
new content snapshot or use the `refresh()` method to update a given `Root`
to the latest state of the content repository:

    Root root = ...;
    root.refresh();

In addition to reading repository content, the client can also make
modifications to the content tree. Such content changes remain local to the
particular `Root` instance (and related subtrees) until explicitly committed.
For example, the following code creates and commits a new subtree containing
nothing but a simple property:

    ContentSession session = ...;
    Root root = session.getCurrentRoot();
    Tree tree = root.getTree("/");
    Tree subtree = tree.addSubtree("hello");
    subtree.setProperty("message", "Hello, World!");
    root.commit();

Even other `Root` instances acquired from the same `ContentSession` won't
see such changes until they've been committed and the other trees refreshed.
This allows a client to track multiple parallel sets of changes with just a
single authenticated session.

