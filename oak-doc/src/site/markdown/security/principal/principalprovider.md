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

Implementations of the PrincipalProvider Interface
--------------------------------------------------------------------------------

Oak contains by default the following implementations of the `PrincipalProvider`
interface:

### org.apache.jackrabbit.oak.security.principal.PrincipalProviderImpl

This is the default implementation of the `PrincipalProvider`, which makes use
of the fact that `Authorizable`s as defined by the Jackrabbit user management
API are always paired with a `Principal`.

The implementation is not tied to a particular user management implementation
and doesn't need to be rewritten if the security setup would be configured with
different implementation of `UserConfiguration`.

### org.apache.jackrabbit.oak.security.user.UserPrincipalProvider

The introduction of the optional `UserConfiguration.getUserPrincipalProvider`
extension allows for an optimized variant of the default principal provider, which
is reading principal information from user and group accounts always paired with
a `Principal` instance.

This extension allows for a provider based implementation details of the user
management implementation it is associated with, offering the ability to optimize
the principal lookup without having to use regular Jackrabbit user management
API calls and the associated overhead in terms of user/group object creation.

While the implementation is located along with the user management implementation
present in Oak this provider implementation should not be considered being
part of the user management API implementation.

Another benefit of this optimized implementation is the ability to specifically
cache the results of the principal resolution in order to improve login performance.
See section [Caching Results of Principal Resolution](cache.html) for further details.

### org.apache.jackrabbit.oak.spi.security.principal.CompositePrincipalProvider

This implementation is a simple wrapper implementation that combines different
principals from different source providers. It is used in
`CompositePrincipalConfiguration` held by the default `SecurityProvider` to
collect all configured/plugged principal configurations i.e. the various
implementations of principal management.

