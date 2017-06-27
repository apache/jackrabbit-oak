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

Combining Multiple Authorization Models
--------------------------------------------------------------------------------

### General Notes

Since Oak 1.4 it is possible to combine multiple authorization models within the
default security setup.

The main entry point for the aggregation of multiple authorization models is the
[CompositeAuthorizationConfiguration], which is in charge of generating composite 
variants of the `AccessControlManager`, `PermissionProvider` and `RestrictionProvider` 
if multiple authorization modules have been configured (see section [Implementation Details](#details) below.

_Please note:_
Despite the fact that Oak supports the aggregation of multiple authorization 
models, this extension is only recommended for experts that have in-depth
knowledge and understanding of Jackrabbit/Oak authorization concepts. Doing so 
might otherwise result in severe security issues and heavily impact overall performance.

<a name="api_extensions"/>
### API Extensions

There are two interfaces required to make a given authorization model deployable 
in an aggregated setup:

- [PolicyOwner]: Extension to the `AccessControlManager`, that allows a given implementation to claim responsibility for handling certain `AccessControlPolicy` implementations.
- [AggregatedPermissionProvider]: Subclass of `PermissionProvider` which is mandated for permission evaluation once multiple providers are configured.

#### PolicyOwner

Interface defining a single method, which allows to identify the `AccessControlManager` 
implementation responsible set or removing a given policy. `AccessControlManager`s 
deployed in a composite authorization setup are required to implement this interface 
_if_ they choose to support _setPolicy_ or _removePolicy_. Omitting the `PolicyOwner` 
interface will most likely result in a `AccessControlException` as no policy owner 
can be found for the given type of policy.

#### AggregatedPermissionProvider

Extension of the `PermissionProvider` interface that allows a given provider to 
be deployed in a composite authorization setup. The additional methods allow the 
aggregating provider to identify if a given `PermissionProvider` can handle permission 
evaluation for a given set of permissions at a given path.

##### Example

The permission provider shipped with the [oak-authorization-cug](cug.html#details) module 
has a very limited scope: it only evaluates read-access to regular items at the 
configured supported paths. This means e.g. that the implementation is not able to 
determine if write access is granted to a given set of `Principal`s and indicates 
this fact by just returning the subset of supported read permissions upon 
`supportedPermissions(Tree, PropertyState, long)`. The aggregated permission provider 
will consequently not consult this implementation for the evaluation of write 
permissions and move on to other providers in the aggregate.

<a name="details"/>
### Implementation Details

As soon as multiple authorization models are configured with the security setup, 
the `CompositeAuthorizationConfiguration` will return a dedicated `JackrabbitAccessControlManager` 
and `PermissionProvider` that are wrapping the objects provided by the aggregated 
implementations.

Note: as long as a single authorization model is configured (default setup) the 
`CompositeAuthorizationConfiguration` will omit any extra wrapping.

#### Access Control

Once multiple modules are deployed a [CompositeAccessControlManager] with the following 
characteristics will be returned:

- API calls reading information will return the combined result of the wrapped implementations. 
- Methods defined solely by `JackrabbitAccessControlManager` additionally test for the delegatees to implement that extension.
- API calls writing back policies will look for the responsible `PolicyOwner` and specifically delegate the call. If no owner can be found an `AccessControlException` is thrown. 

Hence, a given authorization model is free to implement JCR `AccessControlManager` 
or the Jackrabbit extension.

If a given model allows for writing access control policies, the access control manager
implementation must additionally implement the `PolicyOwner` interface as explained in 
section [API Extensions](#api_extensions) above.

#### Permission Evaluation

Only models implementing the `AggregatedPermissionProvider` extensions will be 
respected for aggregation into the [CompositePermissionProvider]. This allows 
individual models to cover only a subset of permissions and|or a subset of paths 
within the repository.

The composite wrapper subsequently applies the following logic to evaluate the 
effective permissions:

- each delegatee is in ask to evaluate the subset of supported permissions if it 
  claims responsible for the given item/path,
- a delegatee that doesn't handle any of the permissions in question it is ignored,
- a delegatee that doesn't claim responsible for the item/path is ignored,
- a given set of permissions is ultimately granted for a given item/path, if _all_ 
  permissions have been successfully processed and none of the delegatees involved 
  denied access.
  
This implies that evaluation of permissions across multiple implementations is 
strictly additive: as soon as one provider denies access (either by an explicit 
deny or by a missing explicit allow) permissions are denied.

Similarly, if a given combination of permission providers fails to process the 
complete set of permissions (e.g. one permission is not covered by any of the modules) 
the access will be denied as none of the provider was in charge of proper evaluation.

For a given permission provider this means: Calling the same method outside of 
the context of the aggregation (i.e. single model setup), a 'limited' provider must 
never grant access for permissions or items it isn't able to handle properly. 
In other words: permissions that have not been explicitly granted within the scope 
of an implementation must be denied.

#### Restriction Management

Support for multiple restriction providers has already been been present with the 
default authorization implementation since Oak 1.0. The mechanism described in 
section [Restriction Management](restriction.html) is not affected by the new functionality.

The `CompositeAuthorizationConfiguration` is in charge of collecting 
the `RestrictionProvider`s from the aggregated modules and expose the complete 
set of restrictions in order to meet the API contract. 

Nevertheless, each authorization model is responsible for exposing, validating and 
evaluating the subset of restrictions it can handle through the access control API 
extensions and the permission evaluation, respectively. A given model may decide to 
provide no support for restrictions. Examples include modules that deal with different 
types of `AccessControlPolicy` where restriction management doesn't apply (see for example [oak-authorization-cug](cug.html#details)).
               
<a name="configuration"/>
### Configuration

By default the `CompositeAuthorizationConfiguration` aggregates results by applying an `AND` operation to the current set of providers.
This can be changed via configuration to an `OR`. See section [Introduction to Oak Security](../../introduction.html#configuration) for further details.

<a name="pluggability"/>
### Pluggability

The following steps are required to plug an additional authorization model into 
the Oak repository:

- Implement your custom `AuthorizationConfiguration`
- Deploy the bundle containing the implementation
- Bind your `AuthorizationConfiguration` to the `SecurityProvider`:
    - in an OSGi setup this is achieved by adding the configuration to the 
      `requiredServicePids` property of the `SecurityProviderRegistration` _("Apache Jackrabbit Oak SecurityProvider")_ 
      i.e. forcing the recreation of the `SecurityProvider`.
    - in a non-OSGi setup this requires adding the configuration 
      to the `SecurityProvider` (e.g. _SecurityProviderImpl.bindAuthorizationConfiguration_) 
      and subsequently creating the JCR/Oak repository object.
         
**Important Note**  
Despite the fact that Oak supports the aggregation of multiple authorization 
models, this extension is only recommended for experts that have in-depth
knowledge and understanding of Jackrabbit/Oak authorization concepts. Doing so 
might otherwise result in severe security issues and heavily impact overall performance.
          
<!-- hidden references -->
[PolicyOwner]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/accesscontrol/PolicyOwner.html
[AggregatedPermissionProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/permission/AggregatedPermissionProvider.html
[CompositeAuthorizationConfiguration]: http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/security/authorization/composite/CompositeAuthorizationConfiguration.java
[CompositeAccessControlManager]: http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/security/authorization/composite/CompositeAccessControlManager.java
[CompositePermissionProvider]: http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/oak-core/src/main/java/org/apache/jackrabbit/oak/security/authorization/composite/CompositePermissionProvider.java