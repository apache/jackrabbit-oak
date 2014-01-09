Oak HTTP binding
================

This is a simple JavaScript- and browser-friendly HTTP binding for Oak.
It makes it possible to access and modify content in an Oak repository
remotely via the HTTP protocol.

The easiest way to try out this binding is to start an in-memory repository
using the `oak-run` jar:

    $ java -jar oak-run/target/oak-run-*.jar

The resulting reposistory is made available at http://localhost:8080/.
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
