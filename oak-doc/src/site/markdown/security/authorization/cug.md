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

- [CugPolicy] marker interface extending `PrincipalSetPolicy`
- [CugExclude]

#### CugExclude

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

### Implementation Details

_todo_

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

_todo_

#### Configuration Parameters of the CugConfiguration

The `org.apache.jackrabbit.oak.spi.security.authorization.cug.impl.CugConfiguration` 
supports the following configuration parameters:

| Parameter                   | Type           | Default  | Description |
|-----------------------------|----------------|----------|-------------|
| `PARAM_CUG_ENABLED`         | boolean        | false    | Flag to enable evaluation of CUG policies upon read-access.  |
| `PARAM_CUG_SUPPORTED_PATHS` | Set\<String\>  | \-       | Paths under which CUGs can be created and will be evaluated. |
| `PARAM_RANKING`             | int            | 200      | Ranking within the composite authorization setup.            |
| | | | |

#### Configure the Excluded Principals

The CUG authorization setup can be further customized by enabling the 
`CugExcludeImpl` service with allows to list additional principals that need
to be excluded from the evaluation of restricted areas:

| Parameter                   | Type           | Default  | Description |
|-----------------------------|----------------|----------|-------------|
| `principalNames`            | Set\<String\>  | \-       | Name of principals that are always excluded from CUG evaluation.  |
| | | | |

Note: this is an optional feature that may be used to extend the default exclusion. 
Alternatively, it is possible to plug a custom `CugExclude` implementation matching 
specific needs (see [below](#pluggability).

<a name="pluggability"/>
### Pluggability

#### Deploy CugConfiguration

##### OSGi Setup

_todo: deploying cug-authorization in an oak repository_

##### Non-OSGi Setup

_todo: deploying cug-authorization in an oak repository_

#### Customize CugExclude
 
_todo: customize cug-authorization by providing custom CugExclude implementation_ 

##### Examples

_todo_

<!-- hidden references -->
[Principal]: http://docs.oracle.com/javase/7/docs/api/java/security/Principal.html
[AccessControlPolicy]: http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/security/AccessControlPolicy.html
[CugPolicy]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/cug/CugPolicy.html