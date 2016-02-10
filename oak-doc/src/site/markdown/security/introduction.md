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
    - Authentication s.str.
    - Token Authentication and Token Management
- Authorization
    - Authorization s.str.
    - Privilege Management 
- Principal Management
- User Management

<a name="api_extensions"/>
### API Extensions

The package `org.apache.jackrabbit.oak.spi.security` defines the following interfaces
and base implementations:

- [SecurityProvider]: Main entry point for Oak security.
    - [OpenSecurityProvider]: Rudimentary implementation for test purpose.
- [SecurityConfiguration]: Base interface for all security related configurations. 
    - [ConfigurationBase]: Abstract base implementation of the `SecurityConfiguration` interface. 
    - [CompositeConfiguration]: Abstract base implementation for all composite configurations that allow for aggregation of multiple modules.
- [ConfigurationParameters]: Utility used to pass around parameters and options.
- [Context]: Context information associated with a given `SecurityConfiguration` that allow to identify items defined and maintained by a give security module implementation. 
    
#### SecurityProvider

_TODO 
 The interface allow to access the available `SecurityConfiguration`s
 defining the individual modules. In addition this provider gives access
 to the configuration parameters that apply to the configurations with the
 specified `SecurityConfiguration#getName() name`._
 
 _ TODO: registration of the security provider_

#### SecurityConfiguration
 
 _TODO_
 
 - [AuthenticationConfiguration]  
 - [TokenConfiguration] : multiple
 - [AuthorizationConfiguration] : multiple since Oak 1.4
 - [PrivilegeConfiguration]
 - [PrincipalConfiguration], multiple 
 - [UserConfiguration]
 
 _TODO_
 
<a name="configuration"/>
### Configuration 

_TODO_

<a name="pluggability"/>
### Pluggability

_TODO_
 
<!-- references -->
[SecurityProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/SecurityProvider.html
[OpenSecurityProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/OpenSecurityProvider.html
[SecurityConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/SecurityConfiguration.html
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
