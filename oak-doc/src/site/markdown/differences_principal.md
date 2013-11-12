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

#### 1. Characteristics of the Principal Management Implementation

The default implementation of the principal management API basically corresponds 
to the default in Jackrabbit 2.x and is based on the user management implementation. 
Note however, that as of OAK only a single principal provider is exposed on the 
SPI level (used to be multiple principal providers with the LoginModule configuration 
in Jackrabbit 2.x). See the configuration section below for details.

#### 2. API Extensions

- `PrincipalProvider` [0]: SPI level access to principals known to the repository
which is also used by the default implementation of the `PrincipalManager` interface.
This interface replaces the internal PrincipalProvider interface present in
Jackrabbit 2.x. Note, that principals from different sources can be supported by
using `CompositePrincipalProvider` [1] or a similar implementation that proxies
different sources.

##### Special Principals
- `AdminPrincipal`: Marker interface to identify the principal associated with administrative user(s) [2].
- `EveryonePrincipal`: built-in group principal implementation that has every other valid principal as member [3].
- `SystemPrincipal`: built-in principal implementation to mark system internal subjects [4].

#### 3. Configuration

##### PrincipalConfiguration [5]:
- `getPrincipalManager` -> returns a new instance of o.a.j.api.security.principal.PrincipalManager [6] (see also `JackrabbitSession#getPrincipalManager()`
- `getPrincipalProvider` -> returns a new instance of principal provider. Note, that in contrast to Jackrabbit 2.x the system may only have one single principal provider implementation configured. In order to combine principals from different sources a implementation that properly handles the different sources is required; the `CompositePrincipalProvider` [1] is an example that combines multiple implementations.

#### 4. References

[0] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/principal/PrincipalProvider.java

[1] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/principal/CompositePrincipalProvider.java

[2] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/principal/AdminPrincipal.java

[3] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/principal/EveryonePrincipal.java

[4] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/principal/SystemPrincipal.java

[5] http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/spi/security/principal/PrincipalConfiguration.java

[6] http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/security/principal/PrincipalManager.java