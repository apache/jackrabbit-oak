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

# Repository construction

Oak comes with a simple mechanism for constructing content repositories
for use in embedded deployments and test cases. This article describes this
mechanism. Deployments in managed enviroments like OSGi should use the native
construction/configuration mechanism of the environment.

The core class to use is called `Oak` and can be found in the
`org.apache.jackrabbit.oak` package inside `oak-core`. It takes a
`MicroKernel` instance and wraps it into a `ContentRepository`:

    MicroKernel kernel = ...;
    ContentRepository repository = new Oak(kernel).createContentRepository();

For test purposes you can use the default constructor that
automatically instantiates an in-memory `MicroKernel` for use with the
repository. And if you're only using the test repository for a single
`ContentSession` or just a singe `Root`, then you can shortcut the login
steps by using either of the last two statements below:

    ContentRepository repository = new Oak().createContentRepository();
    ContentSession session = new Oak().createContentSession();
    Root root = new Oak().createRoot();

By default no pluggable components are associated with the created
repository, so all login attempts will work and result in full write
access. There's also no need to close the sessions or otherwise
release acquired resources, as normal garbage collection will take
care of everything.

To add extra functionality like type validation or indexing support,
use the `with()` method. The method takes all kinds of Oak plugins and
adds them to the repository to be created. The method returns the Oak
instance being used, so you can chain method calls like this:

    ContentRepository repository = new Oak(kernel)
        .with(new InitialContent())        // add initial content
        .with(new DefaultTypeEditor())     // automatically set default types
        .with(new NameValidatorProvider()) // allow only valid JCR names
        .with(new SecurityProviderImpl())  // use the default security
        .with(new PropertyIndexHook())     // simple indexing support
        .with(new PropertyIndexProvider()) // search support for the indexes
        .createContentRepository();

As you can see, constructing a fully featured JCR repository like this
will require quite a few plugins. To avoid having to specify them all
whenever constructing a new repository, we also have a class called
`Jcr` in the `org.apache.jakcrabbit.oak.jcr` package in `oak-jcr`. That
class works much like the `Oak` class, but it constructs
`javax.jcr.Repository` instances instead of `ContentRepositories` and
automatically includes all the plugin components needed for proper JCR
functionality:

    MicroKernel kernel = ...;
    Repository repository = new Jcr(kernel).createRepository();

The `Jcr` class supports all the same `with()` methods as the `Oak` class
does, so you can easily extend the constructed JCR repository with custom
functionality if you like. For test purposes the `Jcr` class also has an
empty default constructor that works like the one in the `Oak` class.

