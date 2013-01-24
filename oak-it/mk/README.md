MicroKernel integration tests
=============================

This component contains integration tests for the `MicroKernel` interface
as defined in the `oak-mk-api` component. The test suite is by default executed
against the default MicroKernel implementation included in that same
component, but you can also use this test suite against any other
implementations as described below.

Testing a MicroKernel implementation
------------------------------------

Follow these four steps to to set up this integration test suite against
a particular MicroKernel implementation.

First, you need to add this `oak-it-mk` component as a test dependency
in the component that contains your MicroKernel implementation:

    <dependency>
      <groupId>org.apache.jackrabbit</groupId>
      <artifactId>oak-it-mk</artifactId>
      <version>...</version>
      <scope>test</scope>
    </dependency>

Second, you need a JUnit test class that runs the full suite of MicroKernel
tests included in this component:

    import org.junit.runner.RunWith;
    import org.junit.runners.Suite;

    import org.apache.jackrabbit.mk.test.MicroKernelTestSuite;

    @RunWith(Suite.class)
    @Suite.SuiteClasses({ MicroKernelTestSuite.class })
    public class EverythingIT {
    }

Third, you need to implement the `MicroKernelFixture` interface in a class
with a public default constructor:

    package my.package;
    public class MyCustomMicroKernelFixture implements MicroKernelFixture {
        ...
    }

Fourth, and finally, you need to list this fixture class in a
`org.apache.jackrabbit.mk.test.MicroKernelFixture` file within the
`META-INF/services/` folder inside your test classpath:

    my.package.MyCustomMicroKernelFixture

License
-------

(see the top-level [LICENSE.txt](../LICENSE.txt) for full license details)

Collective work: Copyright 2012 The Apache Software Foundation.

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
