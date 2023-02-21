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

# Best Practices for External Authentication

<!-- MACRO{toc} -->

## Before you get started

Before you get started make sure you are familiar with the basic concepts of JCR authentication, and its implementation in Apache Jackrabbit Oak.

External authentication in Oak refers to integrating a third party identity provider like LDAP or SAML into the authentication setup optionally combining it with other built-in authentication mechanisms.

## Best Practices 

### JAAS Setup

When combining external authentication with other built-in or custom [login modules] make sure to define a [configuration] with the optimal order and the proper [control flag] for each module to cover all cases. The order should be chosen such that optional and sufficient login modules come first. Potentially expensive authentication against a third party identity provider as well as those for rare use cases should be defined with a lower ranking. 

Additional reading: https://docs.oracle.com/en/java/javase/11/security/appendix-b-jaas-login-configuration-file.html#GUID-7EB80FA5-3C16-4016-AED6-0FC619F86F8E

#### Combination with Token Authentication

Whenever JCR sessions created with Oak are short-lived (e.g. only lasting for a single HTTP request) authentication against an external IDP may not perform well. It is therefore recommended to use external authentication in combination with an additional authentication mechanism like e.g. the built-in [token login](../tokenmanagement.html).

Make sure the token login module has [control flag] 'SUFFICIENT' and is evaluated prior to the external login that connects to the external IDP.

#### Combination with Default Authentication

Oak comes with a default login for user accounts stored and managed inside the JCR content repository. This also includes support for default users like 'anonymous' (guest) and 'admin' with full access to the repository. If this is desired, it is recommend to also add the [default `LoginModule`](../default.html#uid_pw) to the JAAS configuration.

The optional order depends on the frequency of default vs external login: if login or impersonation against local users occurs frequently (e.g. unauthentication login with [GuestCredentials]) the default login module should have a higher ranking. However, if authentication of local users is unlikely, the external oak login should have a ranking.

##### Example JAAS Configuration

The following JAAS configuration is an example when running an Oak repository with external authentication in combination with Apache Sling:

| Ranking | Control Flag | LoginModule Class Name |
|---------|--------------|------------------------|
| 300     | OPTIONAL     | org.apache.jackrabbit.oak.spi.security.authentication.GuestLoginModule |
| 200     | SUFFICIENT   | org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule | 
| 150     | SUFFICIENT   | org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModuleFactory |
| 100     | SUFFICIENT   | org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl |

See [Authentication with External Login Module : Examples](externallogin_examples.html#Integration_with_Standard_Oak_Authentication_used_for_Apache_Sling) for a detailed explanation as well as alternative 
configurations.

### Synchronization of Users and Groups

The external authentication module in Oak comes with the option to synchronize external identities into the content repository (see section [User and Group Synchronization](../usersync.html)).

The following best practices should be followed:

#### External Identity Provider as SSOT

Your external identity provider should be considered the single source of truth (SSOT) for all users and groups defined and managed by it. 

In contrast, the users/groups synchronized into the repository should be considered a volatile cache and ideally are immutable (i.e. only maintained by system sessions in charge of the synchronization). 

The following features provided by the _oak-auth-external_ module help to prevent unintended modification of synchronized external identities:

- [Dynamic Membership](defaultusersync.html#dynamic_membership): Enabling dynamic membership will result in membership information being stored in a protected property that cannot be altered using regular JCR write or Jackrabbit user management API. 
- [Dynamic Group](defaultusersync.html#dynamic_groups): Can be used in combination with dynamic membership when the application requires group principals to also exposed through `UserManager` (and not just through `PrincipalManager` as it would be needed for permission setup). Note though that these group accounts cannot have members added (see section [Enforcing dynamic groups](defaultusersync.html#enforcing_dynamic_groups))
- [Protecting External Identities](defaultusersync.html#protect_external_identities): The module comes with [configuration 
 option](defaultusersync.html#configuration_principals) to protect external identities. If enabled (as warning or as full protection) a dedicated validator that will report/fail attempts to modify synchronized external identities. This will help to identify violations of the immutability contract.
 
#### User Management for External Identities

- Properties and membership for external identities must be managed in the external IDP. Changes made in the JCR repository using user management API will be overwritten upon the next sync.
- Never set a password for external users to make sure uid/pw login gets authenticated against the external IDP and never against the synchronized user in the repository.

#### Membership Nesting

For performance reasons avoid defining unnecessary membership nesting that increase the number of indirections (see [Best Practices for Authorization](../../authorization/bestpractices.html)).

#### Membership crossing IDP boundaries

Introducing membership crossing IDP boundaries should be considered a trust boundary violation.

If adding external identities to local groups cannot be avoided, leverage [conditional auto-membership](defaultusersync.html#configuration_automembership) or auto-membership configuration in combination with dynamic membership (see [Configuration of the DefaultSyncHandler](defaultusersync.html#configuration_sync_handler)).

### Authorization for External Identities

Upon repository login through external authentication the subject is populated with principals obtained from the external identity provider. 

In addition, the configured auto-membership will be resolved for the external user and its external groups (see autoMembership configuration [options](defaultusersync.html#configuration_sync_handler) and section [Automatic Membership with AutoMembershipConfig](defaultusersync.html#configuration_automembership))

The authenticated session will be subject to regular Oak permission evaluation as defined for the instance and described in section [Permissions](../../permission.html). 

#### Access control setup

Synchronized external identities (both in default and in dynamic sync mode) are exposed as principals through the [Principal Management API](../../principal.html) and can be used for access control setup as described in [Access Control Management](../../accesscontrol.html).

See also section [Best Practices for Authorization](../../authorization/bestpractices.html) for recommendations.

External groups get synchronized together with external users upon repository login. If you wish to defined access control setup for groups prior to the synchronization upon login the following 2 options exist:

- Pre-sync external groups to make them available to the principal manager (see next section)
- Configure [ImportMode](../../accesscontrol/default.html#configuration)=`besteffort` with the default Oak authorization setup and define access control content for principals before they exist.
  
#### Pre-sync of external groups

The following 2 options exist to populate the repository with external group principals outside of the regular synchronization upon login:

- The _oak-auth-external_ module comes with a JMX integration that allows for synchronization of external identities outside of the regular repository login. See [JMX Synchronization Tool](../usersync.html#jmx-synchronization-tool) and [SynchronizationMBean](https://jackrabbit.apache.org/oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/impl/jmx/SynchronizationMBean.html) for details. This requires the `ExternalIdentityProvider` to implement the methods required to retrieve external identities. This is the recommended way to pre-sync groups.
- In case the `ExternalIdentityProvider` does not support user and group sync outside of the regular repository login, external identities can be created using Jackrabbit User Management API. Note: 
    - The property `rep:externalId` is system maintained and protected and cannot be added or changed once the group has been persisted.
    - Mistakes in defining the protected properties `rep:externalId`, `rep:authorizableId` or `rep:principalName` will result in a mismatch during authentication, sync and permission evaluation. The only way to fix such mistakes is to remove and recreate the group. Access control content associated with a wrong principal name needs to be removed separately.

<!-- references -->
[login modules]: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/javax/security/auth/spi/LoginModule.html
[configuration]: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/javax/security/auth/login/Configuration.html
[control flag]: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/javax/security/auth/login/AppConfigurationEntry.LoginModuleControlFlag.html
[GuestCredentials]: https://s.apache.org/jcr-2.0-javadoc/javax/jcr/GuestCredentials.html