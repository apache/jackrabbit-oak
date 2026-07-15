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

Managing Access by Principal
--------------------------------------------------------------------------------

### General

Oak 1.16.0 introduces a new, optional authorization model in the `oak-authorization-principalbased` module intended to be 
used in combination with the default implementation present with Apache Jackrabbit Oak. In contrast to the default 
authorization it provides native support for access control management based upon principals.

The model leverages the fact that JSR 283 allows to redefine to scope of a given policy beyond the access controlled node 
it is bound to. Quoting section [16.3 Access Control Policies] of JSR 283:

    Note that the scope of the effect of an access control policy may not be identical to the node to which that policy is bound.

The model is by default disabled and it requires manual [configuration](#configuration) steps in order to add it to 
the Oak security setup. The configuration steps include defining which principals are supported and how to map them 
to an access controlled node in the repository that will hold the policy (see section [API Extensions](#api_extensions)).

<a name="jackrabbit_api"></a>
### Jackrabbit API

Jackrabbit API 2.18 defines an extension of the [AccessControlList] and [JackrabbitAccessControlList] interfaces 
bound to a given [Principal]

- `PrincipalAccessControlList`

The entries contained in this type of ACL are expected to be of type

- `PrincipalAccessControlList.Entry`

which in addition to the methods inherited from  [AccessControlEntry] and [JackrabbitAccessControlEntry] defined the 
absolute path where they will ultimately take effect. See Javadoc for [PrincipalAccessControlList] and [Entry] in 
Jackrabbit API 2.18 for additional details.

<a name="api_extensions"></a>
### API Extensions

The module comes with the following extension in the 
`org.apache.jackrabbit.oak.spi.security.authorization.principalbased` package space:

- [FilterProvider]
- [Filter]

##### FilterProvider and Filter

In order to be operational the principal-based authorization needs to have a `FilterProvider` configured. The corresponding 
`Filter` defines  if the model is able to evaluate permissions for a given set of principals. For any unsupported set of 
principals  permission evaluation will be skipped altogether. Similarly, access control policies can only be obtained and 
modified for supported principals.

Apart from validating principals the `Filter` interface is also responsible for mapping each supported principal to a 
location in the repository where the access control setup for that principal is being stored.

See section [Implementation Details](#details) for a description of the provider implementation present with the module. 
Section [Pluggability](#pluggability) describes how to deploy a custom implementation.                            

<a name="details"></a>
### Implementation Details

<a name="details_access_mgt"></a>
#### Access Control Management

The access control management follows the requirements defined by JSR 283 and the extensions defined by Jackrabbit API 
(see also section [Access Control Management](../accesscontrol.html)).

##### Access Control Policies

The principal-based authorization model returns two types of policies:

- `PrincipalPolicyImpl`: a mutable policy implementating `PrincipalAccessControlList`, which is returned upon 
  `JackrabbitAccessControlManager.getApplicablePolicies(Principal)` and `JackrabbitAccessControlManager.getPolicies(Principal)`.
- `ImmutableACL`: whenever effective policies are returned upon calling `AccessControlManager.getEffectivePolicies(String)` 
   and `JackrabbitAccessControlManager.getEffectivePolicies(Set<Principal>)`.   
 
##### Management by Principal

In order to manage access control the Jackrabbit API extensions need to be used (see [JackrabbitAccessControlManager]). 

- `JackrabbitAccessControlManager.getApplicablePolicies(Principal)`: if the configured `Filter` handles the specified 
   principal this method will return a single empty modifiable policy of type `PrincipalAccessControlList` if no policy 
   has been set for the target principal before.
- `JackrabbitAccessControlManager.getPolicies(Principal)`: once an applicable policy has been set, this method will return 
   a single modifiable policy of type `PrincipalAccessControlList`.
   
The following characteristics apply when modifying the `PrincipalAccessControlList` defined by this implementation:

- all entries will grant access (i.e. no _deny_)
- the effective path parameter must be an absolute JCR path or null for repository level privileges. 
- the effective path may point to a non-existing node.
- the entry may define one or many supported privileges (see `AccessControlManager.getSupportedPrivileges(String absPath)` and **Supported Privileges** below)
- additional restrictions may optionally be specified according to `JackrabbitAccessControlList.getRestrictionNames` (see **Supported Restrictions** below)  
- entries will be added to the list in the order they are created 
- while equal entries will not be added to the list, no additional effort is made to avoid or cleanup redundant entries.
- entries can be reordered within the list (`JackrabbitAccessControlList.orderBefore`) but this doesn't impact the net effect (no denies).

Since `PrincipalAccessControlList` extends `JackrabbitAccessControlList`, new entries can also be added using variants 
of the `addEntry` method. Please note the following principles:

- the specified `Principal` must be equal to the principal for which the policy was obtained
- only _allow_ entries are supported (see above)
- the entry may define one or many supported privileges (see `AccessControlManager.getSupportedPrivileges(String absPath)` and **Supported Privileges** below)
- the new entry must come with a single value _rep:nodePath_ restriction specifying the absolute JCR path where this 
  policy will take effect. To indicate that the entry takes effect at the repository level an empty string value is used.
- additional restrictions may optionally be specified according to `JackrabbitAccessControlList.getRestrictionNames` (see **Supported Restrictions** below)  
                   
The path of the policies (`JackrabbitAccessControlPolicy.getPath`) is defined by the configured `Filter` implementation 
and will be used to persist the modified policy (`AccessControlManager.setPolicy(String, AccessControlPolicy)`) 
or remove it (`AccessControlManager.removePolicy(String, AccessControlPolicy)`).
                   
Both operations require the editing session to have _jcr:modifyAccessControl_ privilege granted at the access 
controlled node that will hold the policy. Since the access control entries contained in the policy will take effect at 
the tree defined by [Entry.getEffectivePath()](http://jackrabbit.apache.org/api/2.18/org/apache/jackrabbit/api/security/authorization/PrincipalAccessControlList.Entry.html#getEffectivePath), 
the editing session **in addition** needs permission to modify access control content at the path defined with each 
individual entry. This contrasts the default implementation where a given entry only takes effect at the tree defined 
by the access controlled node.
           
##### Management by Path
                 
Editing access control by path is not supported with the principal-based access control manager. Consequently, 
`AccessControlManager.getApplicablePolicies(String)` and `AccessControlManager.getPolicies(String)` will  return an empty iterator/array.

Note however that `AccessControlManager.getEffectivePolicies(String)` will make a best-effort approach searching for 
entries that take effect at a given absolute path: the query will look for nodes of type _rep:PrincipalEntry_ that have 
a property _rep:effectivePath_ matching the target path or any of its ancestors. Restrictions limiting the effect 
of a given entry are not taken into account. See also JSR 283 section [16.3.5 Scope of a Policy] in JSR 283.

##### Supported Privileges

All privileges registered with the repository are supported by this authorization model.

##### Supported Restrictions

The principal-based authorization model doesn't come with a dedicated `RestrictionProvider`. Instead it is built to
handle any restriction defined by the Oak authorization setup.

##### Readable Paths

If the principal-based authorization is used together with the default implementation, it will respect the [readable-paths 
configuration](../permission/default.html#configuration). For trees located at or below the readable paths 
`AccessControlManager.getEffectivePolicies(String absPath)` will include a `NamedAccessControlPolicy`.
Note, that in accordance to the default authorization model, this effective policy is not currently not included when 
looking up effective policies by principal.

<a name="details_permission_eval"></a>
#### Permission Evaluation
 
If a given set of principals is supported by the configured `FilterProvider/Filter` implementation, the principal-based 
authorization model will contribute an implementation of `AggregatedPermissionProvider` to the composite. Whether or not 
access will be granted depends on the aggregated providers and their ranking, the composition type and the presence of an 
`AggregationFilter` (see also section [Combining Multiple Authorization Models](composite.html) for details). 

If the set of principals is not supported an `EmptyPermissionProvider` will be returned and the model will be ignored 
altogether. It follows that in this case permission evaluation delegated to other authorization modules configured in the 
composite.

##### Reading and Caching

Once permission evalution is triggered the principal-based model will directly read the effective 
permissions from the persisted access control content. There exists no separate location for permissions like the 
[permission store](../permission/default.html#permissionStore) present with the default implementation.

All entries defined for a given set of principal are eagerly loaded from the access control content and 
kept in memory for each instance of `ContentSession`. This applies to all supported principals irrespective of the size of 
the individual policies or the size of the principal set. 

Note, that the intended use-case for this authorization model expects small sets of system user principals each with a 
limited set of permissions, which result in comparably small ACLs. See [OAK-8227](https://issues.apache.org/jira/browse/OAK-8227) 
for benchmark series that measure read operations with increasing number of entries and principals.

##### Permission Inheritance

In contrast to the default permission evalution the principal-based setup makes no distinction between user and group 
principals nor does't make any assumptions about the order of principals computed and placed in the `Subject` upon login. 
The inheritance model only takes the item hierarchy into account. In other words the evaluation process will 
start at the target item and search up the item hierarchy for a matching entry. An entry is considered matching if it is 
defined for any of the principals in the given set, applies to the target item and grants the specified permissions.  

##### Evaluation Shortcut

As soon as an entry matches the target item and grants the requested permission the evaluation will stop. As this 
model only supports allowing entries there exists no particular requirement to maintain and handle the order of 
entries for different principals that take effect at a given target.

However, in order to minimize excessive read on the `NodeStore` it is recommended to avoid fully redundant entries such as e.g.

- _entry:_ granting privileges, _redundant_: same privileges with additional restrictions
- _entry:_ granting privileges, _redundant:_ subset of these privileges 

##### Readable Paths

Since [OAK-8671](https://issues.apache.org/jira/browse/OAK-8671) principal-based authorization respects the readable 
paths configuration option present with the default authorization model. For any tree located at or below these configured 
paths read-access is always granted.

##### Administrative Access

The principal-based authorization doesn't enforce any special handling for administrative principals. When implementing 
a custom `FilterProvider` this should be part of the considerations. An implementation may e.g. choose not to support 
administrative principals and thus delegate the permission evalution to the default implementation.

##### Permission Evaluation with Multiplexed Stores

This authorization model can be used in combinition with non-default mounts with one notable limitation:
None of the non-default mounts may be located below the configured filter root (see `FilterProvider.getFilterRoot()`) in 
order to make sure all policies defined by this model are located in the same mount.

<a name="details_filterprovider"></a>
#### FilterProvider Implementation

The model comes with an implementation of the `FilterProvider` and `Filter` interfaces which (if enabled) will 
limit the scope of the principal-based authorization according to the following rules:
 
- the set of principals must not be empty and must only contain `SystemUserPrincipal`s
- each `SystemUserPrincipal` must be associated with a location in the repository (i.e. must be `ItemBasedPrincipal` when 
  obtained through principal management API).
- all principals must additionally be located below the path configured with `FilterProviderImpl` (see section [Configuration](#configuration))

So, if this implementation is enabled the principal-based authorization will only take effect for `SystemUserPrincipal`s 
that are created below the configured path. As soon as a given `Subject` or set of principals contains principals that 
doesn't match the filter definition (e.g. group principals, principals not located in the repository or 
located elsewhere in the repository), principal-based authorization will be skipped. This applies both to permission 
evaluation and to access control management.

<a name="details_aggregationfilter"></a>
#### AggregationFilter Implementation
 
In addition principal-based authorization provides a implementation of the [AggregationFilter](composite.html#api_extensions) 
interface that stops the aggregation of `PermissionProvider`s and effective policies as soon as the 
`PrincipalBasedPermissionProvider` takes effect (i.e. the mandatory `FilterProvider` will handle a given set of principals).
The `AggregationFilter` can be enabled by setting the corresponding flag with the module [configuration](#configuration). 

<a name="details_examples"></a>
#### Examples

See [Permission Evaluation with Principal-Based Authorization](principalbased_evaluation.html) for examples illustrating  
an authorization setup including principal-based authorization and how it handles different principals.

<a name="representation"></a>
### Representation in the Repository

The access control lists defined by this module are represented by nodes named `rep:principalPolicy` with primary node 
type `rep:PrincipalPolicy`. The declaring mixin type of this policy node is `rep:PrincipalBasedMixin` (according to 
`rep:AccessControllable`). The policy node has a single mandatory, protected property containing the name of principal 
this policy is bound to and a list of entries of type `rep:PrincipalEntry`. Apart from mandatory privileges and optional 
restrictions each entry defines the target path (`rep:effectivePath`), where it will take effect upon successful commit.

    /**
     * @since oak 1.14
     */
    [rep:PrincipalBasedMixin]
      mixin
      + rep:principalPolicy (rep:PrincipalPolicy) protected IGNORE
    
    /**
     * @since oak 1.14
     */
    [rep:PrincipalPolicy] > rep:Policy
      orderable
      - rep:principalName (STRING) protected mandatory IGNORE
      + * (rep:PrincipalEntry) = rep:PrincipalEntry protected IGNORE
    
    /**
     * @since oak 1.14
     */
    [rep:PrincipalEntry]
      - rep:effectivePath (PATH) protected mandatory
      - rep:privileges (NAME) multiple protected mandatory multiple
      + rep:restrictions (rep:Restrictions) = rep:Restrictions protected
      
_Note:_ While the definition of the `rep:principalName` property doesn't mandate any particular value, it is the mandatory 
`FilterProvider` implementation that will ultimately define, for which types of principals this type of policy can be 
created and where these principals are to be located in the repository.

<a name="validation"></a>
### Validation

The validity of the content structure is asserted by a dedicated `Validator` on creation and modification. The following 
commit failures of type `AccessControl` may occur:

| Code              | Message                                                                                            |
|-------------------|----------------------------------------------------------------------------------------------------|
| 0030              | Attempt create policy node with different name than rep:principalPolicy                            |
| 0031              | Attempt to change primary type from/to rep:PrincipalPolicy                                         |
| 0032              | Reserved node name 'rep:principalPolicy' must only be used for nodes of type 'rep:PrincipalPolicy' |
| 0033              | Parent node not of mixin type 'rep:PrincipalBasedMixin'                                            |
| 0034              | Reserved node name 'rep:restrictions' must only be used for nodes of type 'rep:Restrictions'       |
| 0035              | Invalid restriction                                                                                |
| 0002              | Expected access control entry parent (isolated restriction)                                        |
| 0036              | Isolated entry of principal policy                                                                 |
| 0037              | Empty rep:privileges property                                                                      |
| 0038              | Abstract privilege                                                                                 |
| 0039              | Invalid privilege                                                                                  |

Note, that the validator performs additional checks regarding ability to modify access control content that will take 
effect at the location indicated by the `rep:effectivePath` property. In case the editing session doesn't have sufficient 
permissions at the target location the commit will fail with an error of type `Access`:

| Code              | Message                                                                                         |
|-------------------|-------------------------------------------------------------------------------------------------|
| 0003              | Access denied: If editing session is not granted modify access content at effective target path |

<a name="configuration"></a>
### Configuration

#### Configuration Parameters

The `org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.PrincipalBasedAuthorizationConfiguration` 
defines the following configuration parameters:

| Parameter                         | Type    | Default  | Description                                       |
|-----------------------------------|---------|----------|---------------------------------------------------|
| `PARAM_ENABLE_AGGREGATION_FILTER` | boolean | false    | Flag to enable the aggregration filter.           |
| `PARAM_RANKING`                   | int     | 500      | Ranking within the composite authorization setup. |

The principal-based authorization in addition requires a `FilterProvider` to be configured along side with it in order 
to be operational (mandatory reference in an OSGi setup). This could either be the example implementation present with 
the module or a custom implementation.     

#### FilterProvider Implementation

The `FilterProvider` implementation present with the module limits the effect to system users principals located 
below the configured subtree. The absolute path of this subtree is a mandatory configuration option with the 
`Apache Jackrabbit Oak Filter for Principal Based Authorization` (_ConfigurationPolicy.REQUIRE_):
            
| Parameter | Type   | Default | Description                                                                                          |
|-----------|--------|---------|------------------------------------------------------------------------------------------------------|
| `Path`    | String | \-      | Required path underneath which all filtered system user principals must be located in the repository.|

_Note:_ It is equally possible to plug a custom `FilterProvider` implementation matching specific needs (see [below](#pluggability)).

<a name="pluggability"></a>
### Pluggability

The following section describes how to deploy this authorization model into an Oak repository and how to customize the 
`FilterProvider` extension point.

#### Deploy PrincipalBasedAuthorizationConfiguration

##### OSGi Setup

The following steps are required in order to deploy the CUG authorization model
in an OSGi-base Oak repository:

1. Deploy the `oak-authorization-principalbased` bundle
2. Configure and activate the built-in `FilterProvider` or deploy a custom implementation (see below).
3. Make sure you have the default or a custom `MountInfoProvider` service running
2. Optionally configure the `PrincipalBasedAuthorizationConfiguration` _("Apache Jackrabbit Oak Principal Based AuthorizationConfiguration")_ 
3. Find the `SecurityProviderRegistration` _("Apache Jackrabbit Oak SecurityProvider")_ configuration and 
   enter _`org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.PrincipalBasedAuthorizationConfiguration`_ as 
   additional value to the `requiredServicePids` property.

The third step will enforce the recreation of the `SecurityProvider` and hence 
trigger the `RepositoryInitializer` provided by the principal-based authorization module, that will make sure the
required node type definitions are installed.

##### Non-OSGi Setup

The following example shows a simplified setup that contains the `PrincipalBasedAuthorizationConfiguration` 
as additional authorization model (second position in the aggregation). See also 
unit tests for an alternative approach.

     // setup PrincipalBasedAuthorizationConfiguration
     FilterProvider filterProvider = TODO: define the filter provider;
     MountInfoProvider mip = Mounts.defaultMountInfoProvider();
                     
     PrincipalBasedAuthorizationConfiguration ac = new PrincipalBasedAuthorizationConfiguration();
     ac.bindFilterProvider(filterProvider);
     ac.bindMountInfoProvider(mip);
     // optionally set configuration parameters: ranking, enable aggregationfilter
     
     // bind it to the security provider
     ConfigurationParameters securityConfig = ConfigurationParameters.EMPTY; // TODO define security config options
     SecurityProvider securityProvider = SecurityProviderBuilder.newBuilder().with(securityConfig)
                          .withRootProvider(rootProvider)
                          .withTreeProvider(treeProvider)
                          .build();
     SecurityProviderHelper.updateConfig(securityProvider, ac, AuthorizationConfiguration.class);
                                   
     // create the Oak repository (alternatively: create the JCR repository)
     Oak oak = new Oak()
             .with(new InitialContent())
             // TODO: add all required editors
             .with(securityProvider);
             withEditors(oak);
     ContentRepository contentRepository = oak.createContentRepository();

#### Customize FilterProvider
 
The following steps are required in order to customize the `FilterProvider` implementation
in a OSGi-based repository setup. Ultimately the implementation needs to be referenced 
in the `org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.PrincipalBasedAuthorizationConfiguration`.

1. implement `FilterProvider` and `Filter` interface according to you needs,
2. make your `FilterProvider` implementation an OSGi service
3. deploy the bundle containing your implementation in the OSGi container and activate the service. 

<!-- hidden references -->
[Principal]: http://docs.oracle.com/javase/7/docs/api/java/security/Principal.html
[AccessControlList]: https://s.apache.org/jcr-2.0-javadoc/javax/jcr/security/AccessControlList.html
[AccessControlEntry]: https://s.apache.org/jcr-2.0-javadoc/javax/jcr/security/AccessControlEntry.html
[FilterProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/principalbased/FilterProvider.html
[Filter]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/principalbased/Filter.html
[JackrabbitAccessControlManager]: http://jackrabbit.apache.org/api/2.18/index.html?org/apache/jackrabbit/api/security/JackrabbitAccessControlManager.html
[JackrabbitAccessControlList]: http://jackrabbit.apache.org/api/2.18/index.html?org/apache/jackrabbit/api/security/JackrabbitAccessControlList.html
[JackrabbitAccessControlEntry]: http://jackrabbit.apache.org/api/2.18/index.html?org/apache/jackrabbit/api/security/JackrabbitAccessControlEntry.html
[PrincipalAccessControlList]: http://jackrabbit.apache.org/api/2.18/index.html?org/apache/jackrabbit/api/security/authorization/PrincipalAccessControlList.html
[Entry]: http://jackrabbit.apache.org/api/2.18/org/apache/jackrabbit/api/security/authorization/PrincipalAccessControlList.Entry.html
[16.3 Access Control Policies]: https://s.apache.org/jcr-2.0-spec/16_Access_Control_Management.html#16.3%20Access%20Control%20Policies
[16.3.5 Scope of a Policy]: https://s.apache.org/jcr-2.0-spec/16_Access_Control_Management.html#16.3.5%20Scope%20of%20a%20Policy