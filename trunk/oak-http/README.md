Oak HTTP binding
================

This is a simple JavaScript- and browser-friendly HTTP binding for Oak.
It makes it possible to access and modify content in an Oak repository
remotely via the HTTP protocol.

The easiest way to try out this binding is to start an in-memory repository
using the `oak-run` jar:

    $ java -jar oak-run/target/oak-run-*.jar

The resulting repository is made available at http://localhost:8080/.
The following examples use the httpie client to perform simple CRUD operations.
The username and password (by default `admin` and `admin`) have been specified
in the `~/.netrc` file so they are not included on the command lines.

    $ http -j -b localhost:8080
    {
        "jcr:primaryType": "rep:root",
        "jcr:system": {},
        "oak:index": {},
        "rep:security": {}
    }

You can add or modify content by posting JSON:

    $ http -j -b localhost:8080/test \
          jcr\\:primaryType=oak:Unstructured foo=abc bar:=123
    {
        "bar": "123",
        "foo": "abc",
        "jcr:primaryType": "oak:Unstructured"
    }

The `jcr:primaryType` property needs to currently be included to avoid
JCR constraint violations. The intention is to automatically infer the
value if not explicitly included. Note also how the first colon needs to
be escaped on the command line to prevent httpie from interpreting the
argument as an extra HTTP header.

The posted content is stored in the repository and is now accessible:

    $ http -j -b localhost:8080
    {
        "jcr:primaryType": "rep:root",
        "jcr:system": {},
        "oak:index": {},
        "rep:security": {},
        "test": {}
    }

    $ http -j -b localhost:8080/test
    {
        "bar": "123",
        "foo": "abc",
        "jcr:primaryType": "oak:Unstructured"
    }

You can modify the content by posting more data to the same URL:

    $ http -j -b localhost:8080/test \
          foo=xyz child:='{"jcr:primaryType": "oak:Unstructured"}'
    {
        "bar": "123",
        "child": {},
        "foo": "xyz",
        "jcr:primaryType": "nt:unstructured"
    }

    $ http -j -b localhost:8080/test/child
    {
        "jcr:primaryType": "oak:Unstructured"
    }

Finally, content can be removed either by posting a null value to it
or by using DELETE:

    $ http -j -b localhost:8080/test bar:=null child:=null
    {
        "foo": "xyz",
        "jcr:primaryType": "nt:unstructured"
    }

    $ http -j -h DELETE localhost:8080/test
    HTTP/1.1 200 OK

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
