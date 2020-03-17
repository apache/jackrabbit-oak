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

Principal Management
--------------------------------------------------------------------------------

<a href="jcr_api"/>
### JCR API

JCR itself doesn't come with a dedicated principal management API. Nevertheless
the specification mentions `java.security.Principal` as key feature for access 
control management but leaves the discovery of principals to the implementation 
(see [Section 16.5.7](http://www.day.com/specs/jcr/2.0/16_Access_Control_Management.html#16.5.7%20Principal%20Discovery)).

Therefore an API for principal management has been defined as part of the
extensions present with Jackrabbit API.

<a name="jackrabbit_api"/>
### Jackrabbit API

The Jackrabbit API provides support for principal management (i.e. discovery) that 
are missing in JCR. The relevant interfaces are defined in the 
`org.apache.jackrabbit.api.security.principal' package space:

- `PrincipalManager`
- `PrincipalIterator`
- `JackrabbitPrincipal` extends [Principal](http://docs.oracle.com/javase/7/docs/api/java/security/Principal.html)
    - `ItemBasedPrincipal`

#### Differences wrt Jackrabbit 2.x

See the corresponding [documentation](principal/differences.html).

<a name="api_extensions"/>
### API Extensions

- [PrincipalProvider]: SPI level access to principals known to the repository
which is also used by the default implementation of the `PrincipalManager` interface.
This interface replaces the internal `PrincipalProvider` interface present in
Jackrabbit 2.x. Note, that principals from different sources can be supported by
using [CompositePrincipalProvider] or a similar implementation that proxies
different sources.
- [CompositePrincipalProvider]: Implementation that combines different principals
from different source providers.

##### Special Principals
- [AdminPrincipal]: Marker interface to identify the principal associated with administrative user(s).
- [EveryonePrincipal]: built-in group principal implementation that has every other valid principal as member.
- [SystemPrincipal]: built-in principal implementation to mark system internal subjects.
- [SystemUserPrincipal]: Marker interface to identify principals associated with special system users.

<a href="default_implementation"/>
### Oak Principal Management Implementation

The default implementation of the principal management API basically corresponds
to the default in Jackrabbit 2.x and is based on the user management implementation.
Note however, that as of Oak only a single principal provider is exposed on the
SPI level (used to be multiple principal providers with the LoginModule configuration
in Jackrabbit 2.x). See the configuration section below for details.

#### PrincipalProvider Implementations

See section [Implementations of the PrincipalProvider Interface](principal/principalprovider.html)
for details.

<a name="configuration"/>
### Configuration

The [PrincipalConfiguration] is the Oak level entry point to obtain a new
[PrincipalManager] or [PrincipalProvider] as well as principal related configuration
options. The default implementation of the [PrincipalManager] interface is based
on Oak API and can equally be used for privilege related tasks in the Oak layer.

In contrast to Jackrabbit 2.x the system may only have one single principal
provider implementation configured. In order to combine principals from different
sources a implementation that properly handles the different sources is required;
the [CompositePrincipalProvider] is an example that combines multiple implementations.

<a name="pluggability"/>
### Pluggability

The default security setup as present with Oak 1.0 is able to provide custom
`PrincipalConfiguration` implementations and will automatically combine the different
principal provider implementations as noted above.

In an OSGi setup the following steps are required in order to add a custom principal
provider implementation:

- implement `PrincipalProvider` interface
- create the `PrincipalConfiguration` that exposes the custom provider
- make the configuration implementation an OSGi service and make it available to the Oak repository.

##### Examples

###### Custom PrincipalConfiguration

     @Component()
     @Service({PrincipalConfiguration.class, SecurityConfiguration.class})
     public class MyPrincipalConfiguration extends ConfigurationBase implements PrincipalConfiguration {

         public MyPrincipalConfiguration() {
             super();
         }

         public MyPrincipalConfiguration(SecurityProvider securityProvider) {
             super(securityProvider, securityProvider.getParameters(NAME));
         }

         @Activate
         private void activate(Map<String, Object> properties) {
             setParameters(ConfigurationParameters.of(properties));
         }


         //---------------------------------------------< PrincipalConfiguration >---
         @Nonnull
         @Override
         public PrincipalManager getPrincipalManager(Root root, NamePathMapper namePathMapper) {
             PrincipalProvider principalProvider = getPrincipalProvider(root, namePathMapper);
             return new PrincipalManagerImpl(principalProvider);
         }

         @Nonnull
         @Override
         public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
             return new MyPrincipalProvider(root, namePathMapper);
         }

         //----------------------------------------------< SecurityConfiguration >---
         @Nonnull
         @Override
         public String getName() {
             return NAME;
         }
     }

###### Custom PrincipalProvider

     final class MyPrincipalProvider implements PrincipalProvider {

         MyPrincipalProvider(Root root, NamePathMapper namePathMapper) {
              ...
         }

         ...
     }

<a name="further_reading"/>
### Further Reading

- [Differences wrt Jackrabbit 2.x](principal/differences.html)
- [Implementations of the PrincipalProvider Interface](principal/principalprovider.html)
    - [Caching Results of Principal Resolution](principal/cache.html)

<!-- references -->

[PrincipalManager]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/security/principal/PrincipalManager.java
[PrincipalProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/PrincipalProvider.html
[CompositePrincipalProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/CompositePrincipalProvider.html
[AdminPrincipal]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/AdminPrincipal.html
[EveryonePrincipal]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/EveryonePrincipal.html
[SystemPrincipal]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/SystemPrincipal.html
[SystemUserPrincipal]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/SystemUserPrincipal.html
[PrincipalConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/PrincipalConfiguration.html
