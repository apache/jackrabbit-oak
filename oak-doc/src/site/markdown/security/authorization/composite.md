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
if multiple authorization modules have been configured (see section 
[Implementation Details](#details) below.

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

#### Access Control

_TODO_

#### Permission Evaluation

_TODO_

#### Restriction Management

Support for multiple restriction providers has already been been present with the 
default authorization implementation since Oak 1.0. The mechnism described in 
section [Restriction Management](restriction.html) is not affected by the new functionality.

The `CompositeAuthorizationConfiguration` is in charge of collecting 
the `RestrictionProvider`s from the aggregated modules and expose the complete 
set of restrictions in order to meet the API contract. 

Nevertheless, each authorization model is responsible for exposing, validating and 
evaluating the subset of restrictions it can handle through the access control API 
extensions and the permission evaluation, respectively. Similarly, a given module  
may decide to provide no support for restrictions. Examples include modules that 
provide different types of `AccessControlPolicy` where restrictions cannot be applied 
(see for example [oak-authorization-cug](cug.html#details)).
               
<a name="configuration"/>
### Configuration

There are no implementation specific configuration options associated with 
the `CompositeAuthorizationConfiguration`.

<a name="pluggability"/>
### Pluggability

_TODO_

<!-- hidden references -->
[PolicyOwner]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/accesscontrol/PolicyOwner.html
[AggregatedPermissionProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/permission/AggregatedPermissionProvider.html
[CompositeAuthorizationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/security/authorization/composite/CompositeAuthorizationConfiguration.html