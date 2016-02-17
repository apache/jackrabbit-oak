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

Introduction to Oak Security
--------------------------------------------------------------------------------

### General

The main entry point to Oak security is the `SecurityProvider`, which is registered
to the Oak repository upon creation. The provider is in charge of collecting and 
exposing all security related modules present in a given Oak repository. 

Each security module comes with one or multiple `SecurityConfiguration`(s) that 
is registered with the provider, identified (and possibly aggregated) by their
name.

### Modules

Currently Oak comes with the following built-in security modules, each defined 
by a dedicated sub-interfaces of [SecurityConfiguration]:

- Authentication
    - [Authentication](authentication.html) s.str.
    - [Token Authentication and Token Management](authentication/tokenmanagement.html)
- Authorization
    - [Authorization](authorization.html) s.str.
    - [Privilege Management](privilege.html) 
- [Principal Management](principal.html)
- [User Management](user.html)

<a name="api_extensions"/>
### API Extensions

The package `org.apache.jackrabbit.oak.spi.security` defines the following interfaces
and base implementations:

- [SecurityProvider]: Main entry point for Oak security.
    - [OpenSecurityProvider]: Rudimentary implementation for test purpose with the minimal required setup.
- [SecurityConfiguration]: Base interface for all security related configurations. 
    - [SecurityConfiguration.Default]: Default base implementation return default implementations for initializers, context as well as empty parameters, hooks, validators etc. 
    - [ConfigurationBase]: Abstract base implementation of the `SecurityConfiguration` interface. 
    - [CompositeConfiguration]: Abstract base implementation for all composite configurations that allow for aggregation of multiple modules.
- [ConfigurationParameters]: Utility used to pass around parameters and options.
- [Context]: Context information that allows to identify items defined and maintained by a give security module implementation. 
    
#### SecurityProvider

_TODO_

#### SecurityConfiguration

The `SecurityConfiguration` interface defines functionality common to all security 
related modules. Apart from simple configuration parameters the basic interface 
defines the means to properly initialize a given security module and provide 
Oak internal mechanism to asserts proper validation and data consistency 
for all security relevant operations.

Please note, that `RepositoryInitializer` and `WorkspaceInitializer` as exposed by 
the base interface are only invoked upon repository|workspace initialization and 
consequently impact the [pluggability](#pluggability) of a given security module. 
Examples includes modules that require modifications to the global repository content 
such as node types, namespaces and privileges or require the installation of 
initial content or index definitions for proper execution.
 
The following subinterfaces of `SecurityConfiguration` are currently defined by Oak:

- [AuthenticationConfiguration], **mandatory** 
- [AuthorizationConfiguration], **mandatory**
- [PrincipalConfiguration] 
- [PrivilegeConfiguration]
- [TokenConfiguration]
- [UserConfiguration]
 
##### Mandatory and Optional Modules

While Oak ships default implementation for all security modules listed above, it 
is important to understand that from an Oak point of view only authentication and 
authorization are mandatory for a functional Oak repository.

This is compliant with the security requirements defined by JSR 283 which defines 
API to login into the repository and mandates minimal permission evaluation, 
be it implemenation specific of imposed by the optional access control management.

The minimal security setup may consequently be reduced to a setup as defined by 
the following imaginary, custom `SecurityProvider` (see also [OpenSecurityProvider])

     public class MySecurityProvider implements SecurityProvider {
     
         [...]
     
         public <T> T getConfiguration(Class<T> configClass) {
             if (AuthenticationConfiguration.class == configClass) {
                 return (T) new MyAuthentication();
             } else if (AuthorizationConfiguration.class == configClass) {
                 return (T) new MyAuthorization();
             } else {
                 throw new IllegalArgumentException();
             }
         }
         
         private final class MyAuthentication extends SecurityConfiguration.Default implements AuthenticationConfiguration {   
             [...]
         }
         
         private final class MyAuthorization extends SecurityConfiguration.Default implements AuthorizationConfiguration { 
             public AccessControlManager getAccessControlManager(Root root, NamePathMapper namePathMapper) {
                 throw new UnsupportedOperationException();
             }
             public RestrictionProvider getRestrictionProvider() {
                 throw new UnsupportedOperationException();
             }
             public PermissionProvider getPermissionProvider(Root root, String workspaceName, Set<Principal> principals) {
                 return MyPermissionProvider.getInstance(principals);
             }
         }
     }
 
All other security modules can be considered _optional_ from an Oak repository point 
of view. Please note the following dependencies and special cases:

1. **Authentication** is mandatory and expected to bind a set of `Principal`s to the `Subject` created 
   or updated before|during the repository login.
2. **Permission Evalution** is mandatory and associated with the set of `Principal`s 
   bound to to the `Subject` during the authentication step.
3. `Principal`s represent the link between authentication and authorization and _MAY_ 
   be exposed by Principal Management module as described above.
4. **Access Control Management** is optional and _usually_ goes along with 
    - Principal Management
    - Privilege Management
5. **Principal Management** is optional and is _NOT_ tied to User Management. 
   However, supporting User Management in a given repository setup _usually_ goes 
   along with exposing the corresponding principals as part of the Principal Management.
6. **User Management** is optional and _MAY_ be used for credentials validation during the authentication 
   step. If present it is _usually_ used as a source for principals exposed by Principal Management.
   
<a name="configuration"/>
### Configuration 

_TODO_

<a name="pluggability"/>
### Pluggability

The Oak security setup distinguishes two types security modules:

- Unary modules: `AuthenticationConfiguration`, `PrivilegeConfiguration`, `UserConfiguration`
- Multiple modules: `AuthorizationConfiguration` (since Oak 1.4), `PrincipalConfiguration`, `TokenConfiguration`

#### OSGi setup
_TODO_

#### Non-OSGi setup
_TODO_
 
<!-- references -->
[SecurityProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/SecurityProvider.html
[OpenSecurityProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/OpenSecurityProvider.html
[SecurityConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/SecurityConfiguration.html
[SecurityConfiguration.Default]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/SecurityConfiguration.Default.html
[CompositeConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/CompositeConfiguration.html
[ConfigurationBase]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/ConfigurationBase.html
[ConfigurationParameters]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/ConfigurationParameters.html
[Context]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/Context.html
[AuthenticationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/AuthenticationConfiguration.html
[TokenConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/token/TokenConfiguration.html
[AuthorizationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/AuthorizationConfiguration.html
[PrincipalConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/PrincipalConfiguration.html
[PrivilegeConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/privilege/PrivilegeConfiguration.html
[UserConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/user/UserConfiguration.html
