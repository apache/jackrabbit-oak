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
are registered with the provider, identified (and possibly aggregated) by their
name.

### Modules

Currently Oak comes with the following built-in security modules, each defined 
by a dedicated sub-interfaces of [SecurityConfiguration]:

- Authentication
    - [Authentication](authentication.html) s.str.
    - [Token Authentication and Token Management](authentication/tokenmanagement.html)
- Authorization
    - [Authorization](authorization.html) s.str. including [Access Control Management](accesscontrol.html) and [Permission Evaluation](permission.html)
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
- [RegistrationConstants]: Utility used to define well-known registration properties
    
#### SecurityProvider

The `SecurityProvider` is the key to Oak security by providing access to the 
individual security modules and the configurations associated. Since version 
1.3.7 Oak provides two implementations of the `SecurityProvider` suited for OSGi 
and non-OSGi setup, respectively.

##### OSGi Setup

Since Oak 1.3.7 the core bundle will install a dedicated OSGi component 
([SecurityProviderRegistration], labeled _"Apache Jackrabbit Oak SecurityProvider"_), 
which registers the `SecurityProvider` once all mandatory references have 
successfully been resolved. This new approach addresses issues present with 
the initial security provider implementation and has been backported to existing 
branches (see [OAK-3201] and [OAK-3441]).

While optional configuration setting can be changed or extended at runtime, 
modules and extensions considered required for a functional security setup, need 
to be listed in the _"Required Service PIDs"_ property. This asserts both reliable 
security setup and proper initialization of the individual modules. See also 
sections [Configuration](#configuration) and [Pluggability](#pluggability) below.

##### Non-OSGi Setup

In a non-OSGi setup the `SecurityProvider` (be it the default or a custom implementation) 
gets passed to the repository constructor. See section [pluggability](#pluggability) 
for details wrt module initialization.

The following example has been extracted from the basic test setup:

    NodeStore nodeStore = ...
    
    ConfigurationParameters params = ... // TODO: provide config options
    SecurityProvider sp = new SecurityProviderImpl(params);   
    // Optional: bind additional/custom implementations of the supported `SecurityConfiguration`s 
    
    Repository repository = new Jcr(nodeStore).with(sp).createRepository();

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

While Oak ships default implementation for all security configurations listed above, 
only authentication and authorization are mandatory for a functional Oak repository.

This is compliant with the security requirements defined by JSR 283 which defines 
API to login into the repository and mandates minimal permission evaluation, 
be it implementation specific of imposed by the optional access control management.

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

1. **Authentication** is mandatory and expected to bind a set of `Principal`s to 
   the `Subject`. This may happen before or during the repository login.
2. **Permission Evaluation** is mandatory and associated with the set of `Principal`s 
   bound to to the `Subject` during the authentication step.
3. `Principal`s represent the link between authentication and authorization and _MAY_ 
   be exposed by Principal Management module as described above.
4. **Access Control Management** is optional and _usually_ goes along with Principal 
   and Privilege Management
5. **Principal Management** is optional and is _NOT_ tied to User Management. 
   However, supporting User Management in a given repository setup _usually_ goes 
   along with exposing the corresponding principals as part of the Principal Management.
6. **User Management** is optional and _MAY_ be used for credentials validation during the authentication 
   step. If present it is _usually_ used as a source for principals exposed by Principal Management.

<a name="configuration"/>
### Configuration 

The configuration parameters of individual security modules are described in 
the corresponding sections. The following paragraphs describe the configuration of 
`SecurityProvider` and `CompositeConfiguration` in an OSGi-base setup.

#### SecurityProviderRegistration

| Parameter                | Type     | Default   | Description            |
|--------------------------|----------|-----------|------------------------|
| `Required Services`    | String[] | see below | Service references mandatory for the SecurityProvider registration. |

The value of the individual configuration entries can be one of:

- the value of the `service.pid` registration property
- the value of the `oak.security.name` registration property

By default the `SecurityProviderRegistration` defines the following mandatory services. 
As long as these required references are not resolved the `SecurityProviderRegistration` 
will not register the `SecurityProvider` service and ultimately prevent premature 
initialization of the Oak repository instance.

- "org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl"
- "org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl",
- "org.apache.jackrabbit.oak.security.authentication.token.TokenConfigurationImpl",
- "org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider",
- "org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl",
- "org.apache.jackrabbit.oak.security.user.UserAuthenticationFactoryImpl"

The value of this configuration parameter needs to be adjusted for any additional 
module or functionality that is considered required for a successful security setup.
See section [pluggability](#pluggability) below.

| Parameter                | Type     | Default   | Description            |
|--------------------------|----------|-----------|------------------------|
| `Authorization Composition Type`  | String (AND|OR) | AND | The Composite Authorization model uses this flag to determine what type of logic to apply to the existing providers|

Given a set of permission providers, the composite model can aggregate the results by applying an `AND` logic (for example all providers must allow a specific privilege in order to be granted), or an `OR` (for example any provider can allow a privilege). By default the `AND` version is used.

#### CompositeConfiguration

| Parameter       | Type  | Default                    | Description            |
|-----------------|-------|----------------------------|------------------------|
| `PARAM_RANKING` | int   | `NO_RANKING` (`Integer.MIN_VALUE`) | Optional configuration parameter to define the ranking within the aggregation. |

Note: Security modules that allow for multiple configurations may choose to expose 
the `PARAM_RANKING` option in order to allow for explicit ordering of the individual 
implementations. If the ranking parameter is omitted the `CompositeConfiguration`
will try to use the [SERVICE_RANKING] to define the order. If neither is available 
(or set to `NO_RANKING`) the new entry will be appended to the list.

<a name="pluggability"/>
### Pluggability

In a default setup Oak allows to plug custom or additional implementations of 
the various `SecurityConfiguration` described before. Similarly it would be possible 
to provide a custom `SecurityProvider`.

_Please note_: this is only recommended for experts having in-depth 
understanding of Oak internals and which understand the security risk associated 
with custom replacements or extensions.

#### SecurityProvider

The default `SecurityProvider` service could be replaced by deploying a custom 
service to the OSGi container. In a non-OSGi setup the JCR|Oak repository needs 
to be created with the custom implementation:

    SecurityProvider sp = new MySecurityProvider();  
    Repository repository = new Jcr().with(sp).createRepository();

#### SecurityConfiguration

The default Oak security setup distinguishes between the following types of modules:

- Unary modules: `AuthenticationConfiguration`, `PrivilegeConfiguration`, `UserConfiguration`
- Multiple modules: `AuthorizationConfiguration` (since Oak 1.4), `PrincipalConfiguration`, `TokenConfiguration`

Plugging an implementation of an unary module will replace the default provided by Oak.
As far as the multiple modules are concerned a custom implementation plugged into 
the repository will result in the creation of a [CompositeConfiguration]. The
aggregated modules are kept in a list while the insertion order is defined by the 
`PARAM_RANKING` or by the OSGi service ranking in case the explicit ranking 
parameter is missing.

##### OSGi setup

The following steps are required to replace an existing _unary_ security component
or add an additional implementation (for _multiple_ configurations only):

- Deploy the bundle containing your custom implementation
- Configure the component according to your needs
- For _multiple_ configurations make sure you have set the desired ranking
- Find the `SecurityProviderRegistration` _("Apache Jackrabbit Oak SecurityProvider")_ 
  config and enter the PID of your custom security module as additional 
  value to the `requiredServicePids` property.
- In case of a replacement remove the PID of the module to be replaced and make 
  sure your implementation gets a higher [SERVICE_RANKING] or deactivate the obsolete
  module altogether.

##### Non-OSGi setup

In this case the individual security modules get "manually" bound/unbound to the 
`SecurityProvider` instance. The provider itself might be the default or a custom 
implementation of the interface. If and how security modules can be added to the 
provider remains an implementation detail and is not part of the `SecurityProvider` 
interface definition.

###### Example

Extend the default `SecurityProvider` with a custom `PrincipalConfiguration`.
See also _oak-exercise_ module for an example.

    SecurityProvider sp = new SecurityProviderImpl();  
    sp.bindPrincipalConfiguration(new MyPrincipalConfiguration());    
    Repository repository = new Jcr().with(sp).createRepository();

##### Initialization of SecurityConfiguration(s)

If a given security modules mandates repository and|or workspace initialization 
steps such as e.g. node type registration or persisting new index definitions, the 
deployment of the module requires a reload of the `SecurityProvider`.

In the default OSGi-based setup this is achieved by adding the PID of corresponding 
service  to the `Required Service PIDs` property mentioned above ultimately forcing the 
re-registration of the `SecurityProvider` service with the bundle context. 

Other setup scenarios would need to recreate the `ContentRepository` object or 
adjust the repository 'initial' content _before_ binding the new configuration to 
the `SecurityProvider` in order to avoid inconsistencies. 

<!-- references -->
[SecurityProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/SecurityProvider.html
[OpenSecurityProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/OpenSecurityProvider.html
[SecurityConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/SecurityConfiguration.html
[SecurityConfiguration.Default]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/SecurityConfiguration.Default.html
[CompositeConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/CompositeConfiguration.html
[ConfigurationBase]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/ConfigurationBase.html
[ConfigurationParameters]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/ConfigurationParameters.html
[Context]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/Context.html
[RegistrationConstants]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/RegistrationConstants.html
[AuthenticationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/AuthenticationConfiguration.html
[TokenConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/token/TokenConfiguration.html
[AuthorizationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/AuthorizationConfiguration.html
[PrincipalConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/principal/PrincipalConfiguration.html
[PrivilegeConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/privilege/PrivilegeConfiguration.html
[UserConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/user/UserConfiguration.html
[OAK-3201]: https://issues.apache.org/jira/browse/OAK-3201
[OAK-3441]: https://issues.apache.org/jira/browse/OAK-3441
[SecurityProviderRegistration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/security/internal/SecurityProviderRegistration.html
[SERVICE_RANKING]: https://osgi.org/javadoc/r4v43/core/org/osgi/framework/Constants.html#SERVICE_RANKING