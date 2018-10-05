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
### Principal Management : Differences wrt Jackrabbit 2.x

#### Replacement for Jackrabbit Internals

As of Oak 1.0 the following interfaces and class that were internal to Jackrabbit
have been made part of public API exposed by Oak:

- [org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider]: corresponds to o.a.j.core.security.principal.PrincipalProvider
- [org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal]: corresponds to o.a.j.core.security.principal.AdminPrincipal
- [org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal]: corresponds to o.a.j.core.security.principal.EveryonePrincipal
- [org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal]: corresponds to o.a.j.core.security.SystemPrincipal

#### Combining Principals from Different Sources

In contrast to Jackrabbit 2.x Oak only deals with a single `PrincipalProvider`. In
order to combine principals from different sources a implementation that properly
handles the different sources is required; the [CompositePrincipalProvider] is an
example that combines multiple implementations.

See section [Pluggability](../principal.html#pluggability) for an example.

<!-- references -->

[org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/security/principal/PrincipalManager.java
[CompositePrincipalProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/CompositePrincipalProvider.html
[org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/AdminPrincipal.html
[org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/EveryonePrincipal.html
[org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/SystemPrincipal.html
