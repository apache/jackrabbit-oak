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

External Identity Management
--------------------------------------------------------------------------------

### General

_todo_

### Identity Management API

- [ExternalIdentityProviderManager]: factory for the `ExternalIdentityProvider`
- [ExternalIdentityProvider]: provides user/group information from a third party system.
- [ExternalIdentity]: base interface for an external user/group
    - [ExternalUser]
    - [ExternalGroup]
- [ExternalIdentityRef]: reference to an external user/group

### Default Implementation

The default implementation present with Oak 1.0 allows for third party authentication
against LDAP.

_todo_

The configuration details are described in section [LDAP Integration](ldap.html).

### Pluggability

_todo_

<!-- references -->
[ExternalIdentityProviderManager]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentityProviderManager.html
[ExternalIdentityProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentityProvider.html
[ExternalIdentity]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentity.html
[ExternalUser]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalUser.html
[ExternalGroup]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalGroup.html
[ExternalIdentityRef]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentityRef.html