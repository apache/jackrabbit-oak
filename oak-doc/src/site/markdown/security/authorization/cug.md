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

Managing Access with "Closed User Groups" (CUG)
--------------------------------------------------------------------------------

### General

The `oak-authorization-cug` module provides a alternative authorization model
intended to limit read access to certain paths for a selected, small set of
`Principal`s.

These restricted areas called `CUG` are marked by a dedicated policy type and
effectively prevent read-access for anybody not explicitly allowed.

This implies that the CUG-authorization model solely evaluates and enforces read 
access to regular nodes and properties. Therefore it may only be used as an additional, 
complementary authorization scheme while the primary module(s) is/are in charge 
of enforcing the complete set of permissions including read/write access,
repository operations and any kind of special permissions like reading and
writing access control content. See section [Combining Multiple Authorization Models](composite.html)
for information aggregating access control management and permission evaluation
from different implementations.

By default the `oak-authorization-cug` model is disabled and it requires
manual [configuration](#configuration) steps in order to plug it into the Oak 
security setup.

Once deployed this authorization configuration can be used in the following two 
operation modes:

1. Evaluation disabled: Access control management is supported and policies may
be applied to the repository without taking effect.
2. Evaluation enabled: All policies edited and applied by this module will take
effect upon being persisted, i.e. access to items located in a restricted are
will be subject to the permission evaluation associated with the authorization model.

<a name="jackrabbit_api"/>
### Jackrabbit API

The Jackrabbit API defines an extension of the JCR [AccessControlPolicy] interface 
intended to grant the ability to perform certain actions to a set of
[Principal]s:

- `PrincipalSetPolicy`

See [Jackrabbit API](http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/security/authorization/PrincipalSetPolicy.java) 
for details and the methods exposed by the interface.

<a name="api_extensions"/>
### API Extensions

The module comes with the following extension in the 
`org.apache.jackrabbit.oak.spi.security.authorization.cug` package space:

- [CugPolicy]
- [CugExclude]

##### CugPolicy

The `CugPolicy` interface extends the `PrincipalSetPolicy` and `JackrabbitAccessControlPolicy` 
interfaces provided by Jackrabbit API. It comes with the following set of methods that allow to
read and modify the set of `Principal`s that will be allowed to access the restricted 
area defined by a given policy instance.

    CugPolicy extends PrincipalSetPolicy, JackrabbitAccessControlPolicy
       
       Set<Principal> getPrincipals();       
       boolean addPrincipals(@Nonnull Principal... principals) throws AccessControlException;       
       boolean removePrincipals(@Nonnull Principal... principals) throws AccessControlException;


##### CugExclude

The `CugExclude` allows to customize the set of principals excluded from evaluation
of the restricted areas. These principals will consequently never be prevented 
from accessing any of the configured CUGs and read permission evaluation is 
delegated to any other module present in the setup.

The feature ships with two implementations out of the box:

- `CugExclude.Default`: Default implementation that excludes admin, system and 
system-user principals. It will be used as fallback if no other implementation is configured.
- `CugExcludeImpl`: OSGi service extending from the default that additionally 
allows to excluded principals by their names at runtime.

See also section [Pluggability](#pluggability) below.                            

<a name="details"/>
### Implementation Details

#### Access Control Management

The access control management part of the CUG authorization models follows the
requirements defined by JSR 283 the extensions defined by Jackrabbit API (see section 
[Access Control Management](../accesscontrol.html) with the following characterstics:

##### Supported Privileges

This implemenation of the `JackrabbitAccessControlManager` only supports a subset of
privileges, namely `jcr:read`, `rep:readProperties`, `rep:readNodes`.

##### Access Control Policies

Only a single type of access control policies (`CugPolicy`) is exposed and accepted 
by the access control manager. Once effective each CUG policy creates a restricted
area starting at the target node and inherited to the complete subtree defined
therein. 

Depending on the value of the mandatory `PARAM_CUG_SUPPORTED_PATHS` [configuration](#configuration) 
option creation (and evaluation) of CUG policies can be limited to 
certain paths within the repository. Within these supported paths CUGs can
be nested. Note however, that the principal set defined with a given `CugPolicy`
is not inherited to the nested policies applied in the subtree.

_Note:_ For performance reasons it is recommended to limited the usage of `CugPolicy`s
to a single or a couple of subtrees in the repository.
 
##### Management by Principal

Given the fact that a given CUG policy takes effect for all principals present in
the system, access control management by `Principal` is not supported currently.
The corresponding Jackrabbit API methods always return an empty policy array.

#### Permission Evaluation

As stated above evaluation of the restricted areas requires the `PARAM_CUG_ENABLED` 
[configuration](#configuration) option to be set to `true`. This switch allows to 
setup restricted areas in a staging enviroment and only let them take effect in
the public facing production instance.

If permission evaluation is enabled, the `PermissionProvider` implementation associated 
with the authorization model will prevent read access to all restricted areas
defined by a `CugPolicy`. Only `Principal`s explicitly allowed by the policy itself 
or the globally configured `CugExclude` will be granted read permissions to the 
affected items in the subtree.

For example, applying and persisting a new `CugPolicy` at path _/content/restricted/apache_foundation_, 
setting the principal names to _apache-members_ and _jackrabbit-pmc_ will prevent
read access to the tree defined by this path for all `Subject`s that 
doesn't match any of the two criteria:

- the `Subject` contains`Principal` _apache-members_ and|or _jackrabbit-pmc_ (as defined in the `CugPolicy`)
- the `Subject` contains at least one `Principal` explicitly excluded from CUG evaluation in the configured, global `CugExclude`

This further implies that the `PermissionProvider` will only evaluate regular read 
permissions (i.e. `READ_NODE` and `READ_PROPERTY`). Evaluation of any other 
[permissions](../permission.html#oak_permissions) including reading the cug policy 
node (access control content) is consequently delegated to other 
authorization modules. In case there was no module dealing with these permissions, 
access will be denied (see in section _Combining Multiple Authorization Models_ for [details](composite.html#details)). 

### Representation in the Repository

CUG policies defined by this module in a dedicate node name `rep:cugPolicy` of 
type `rep:CugPolicy`. This node is defined by a dedicate mixin type 
`rep:CugMixin` (similar to `rep:AccessControllable`) and has a single mandatory,
protected property which stores the name of principals that are granted read
access in the restricted area:

    [rep:CugMixin]
      mixin
      + rep:cugPolicy (rep:CugPolicy) protected IGNORE
      
    [rep:CugPolicy] > rep:Policy
      - rep:principalNames (STRING) multiple protected mandatory IGNORE
      
_Note:_ the multivalued `rep:principalNames` property reflects the fact 
that CUGs are intended to be used for small principal sets, preferably 
`java.security.acl.Group` principals. 

<a name="validation"/>
### Validation

The consistency of this content structure both on creation and modification is
asserted by a dedicated `CugValidatorProvider`. The corresponding error are
all of type `AccessControl` with the following codes:

| Code              | Message                                                  |
|-------------------|----------------------------------------------------------|
| 0020              | Attempt to change primary type of/to cug policy          |
| 0021              | Wrong primary type of 'rep:cugPolicy' node               |
| 0022              | Access controlled not not of mixin 'rep:CugMixin'        |
| 0023              | Wrong name of node with primary type 'rep:CugPolicy'     |

<a name="configuration"/>
### Configuration

The CUG authorization extension is an optional feature that requires mandatory
configuration: this includes defining the supported paths and enabling the
permission evaluation.

#### Configuration Parameters

The `org.apache.jackrabbit.oak.spi.security.authorization.cug.impl.CugConfiguration` 
supports the following configuration parameters:

| Parameter                   | Type           | Default  | Description |
|-----------------------------|----------------|----------|-------------|
| `PARAM_CUG_ENABLED`         | boolean        | false    | Flag to enable evaluation of CUG policies upon read-access.  |
| `PARAM_CUG_SUPPORTED_PATHS` | Set\<String\>  | \-       | Paths under which CUGs can be created and will be evaluated. |
| `PARAM_RANKING`             | int            | 200      | Ranking within the composite authorization setup.            |
| | | | |

_Note:_ depending on other the authorization models deployed in the composite 
setup, the number of CUGs used in a given deployment as well as other 
factors such as predominant read vs. read-write, the performance of overall 
permission evaluation may benefit from changing the default ranking of the 
CUG authorization model.

#### Excluding Principals

The CUG authorization setup can be further customized by configuring the 
`CugExcludeImpl` service with allows to list additional principals that need
to be excluded from the evaluation of restricted areas:

| Parameter                   | Type           | Default  | Description |
|-----------------------------|----------------|----------|-------------|
| `principalNames`            | Set\<String\>  | \-       | Name of principals that are always excluded from CUG evaluation.  |
| | | | |

_Note:_ this is an optional feature to extend the [default](/oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/cug/CugExclude.Default.html) 
exclusion list. Alternatively, it is possible to plug a custom `CugExclude` implementation matching 
specific needs (see [below](#pluggability)).

<a name="pluggability"/>
### Pluggability

The following section describes how to deploy the CUG authorization model into
an Oak repository and how to customize the `CugExclude` extension point.

_Note:_ the reverse steps can be used to completely disable the CUG 
authorization model in case it is not needed for a given repository 
installation but shipped by a vendor such as e.g. Adobe AEM 6.3.

#### Deploy CugConfiguration

##### OSGi Setup

The following steps are required in order to deploy the CUG authorization model
in an OSGi-base Oak repository:

1. Deploy the `oak-authorization-cug` bundle
2. Activate the `CugConfiguration` _("Apache Jackrabbit Oak CUG Configuration")_ by providing the desired component configuration (_ConfigurationPolicy.REQUIRE_)
3. Find the `SecurityProviderRegistration` _("Apache Jackrabbit Oak SecurityProvider")_ configuration and 
enter _`org.apache.jackrabbit.oak.spi.security.authorization.cug.impl.CugConfiguration`_ as additional value to the `requiredServicePids` property.

The third step will enforce the recreation of the `SecurityProvider` and hence 
trigger the `RepositoryInitializer` provided by the CUG authorization module.

##### Non-OSGi Setup

The following example shows a simplified setup that contains the `CugConfiguration` 
as additional authorization model (second position in the aggregation). See also 
unit tests for an alternative approach.

     // setup CugConfiguration
     ConfigurationParameters params = ConfigurationParameters.of(AuthorizationConfiguration.NAME,
             ConfigurationParameters.of(ConfigurationParameters.of(
                     CugConstants.PARAM_CUG_SUPPORTED_PATHS, "/content",
                     CugConstants.PARAM_CUG_ENABLED, true)));
     CugConfiguration cug = new CugConfiguration();
     cug.setParameters(params);
     
     // bind it to the security provider (simplified => subclassing required due to protected access)
     SecurityProviderImpl securityProvider = new SecurityProviderImpl();
     securityProvider.bindAuthorizationConfiguration(cug);
     
     // create the Oak repository (alternatively: create the JCR repository)
     Oak oak = new Oak()
             .with(new InitialContent())
             // TODO: add all required editors
             .with(securityProvider);
             withEditors(oak);     
     ContentRepository contentRepository = oak.createContentRepository();     
     
#### Customize CugExclude
 
The following steps are required in order to customize the `CugExclude` implementation
in a OSGi-based repository setup. Ultimately the implementation needs to be referenced 
in the `org.apache.jackrabbit.oak.spi.security.authorization.cug.impl.CugConfiguration`.

1. implement `CugExclude` interface according to you needs,
2. make your implementation an OSGi service
3. deploy the bundle containing your implementation in the OSGi container and activate the service.

###### Example

    @Component()
    @Service(CugExclude.class)
    public class MyCugExclude implements CugExclude {
    
        private static final Principal PRINCIPAL_APACHE_MEMBERS = new PrincipalImpl("apache-members");
        private static final Principal PRINCIPAL_JACKRABBIT_PMC = new PrincipalImpl("jackrabbit_pmc");
    
        public MyCugExclude() {}

        //-----------------------------------------------------< CugExclude >---
        @Override
        public boolean isExcluded(@Nonnull Set<Principal> principals) {
            return principals.contains(PRINCIPAL_APACHE_MEMBERS) || principals.contains(PRINCIPAL_JACKRABBIT_PMC);
        }

        //------------------------------------------------< SCR Integration >---
        @Activate
        private void activate(Map<String, Object> properties) {
        }
    }

<!-- hidden references -->
[Principal]: http://docs.oracle.com/javase/7/docs/api/java/security/Principal.html
[AccessControlPolicy]: http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/security/AccessControlPolicy.html
[CugPolicy]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/cug/CugPolicy.html
[CugExclude]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/cug/CugExclude.html