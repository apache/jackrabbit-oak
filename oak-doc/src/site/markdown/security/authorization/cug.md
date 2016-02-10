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

Managing Access with Closed User Groups (CUG)
--------------------------------------------------------------------------------

### General

_todo_

<a name="jackrabbit_api"/>
### Jackrabbit API

_todo: PrincipalSetPolicy_

<a name="api_extensions"/>
### API Extensions

_todo: CugPolicy_
_todo: CugExclude_

### Characteristics of the default CUG Implementation

_todo_

#### CUG Representation in the Repository

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
##### Validation

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

#### Configuration Parameters

The implementation supports the following configuration parameters:

| Parameter                   | Type           | Default  | Description |
|-----------------------------|----------------|----------|-------------|
| `PARAM_CUG_ENABLED`         | boolean        | false    | Flag to enable evaluation of CUG policies upon read-access.  |
| `PARAM_CUG_SUPPORTED_PATHS` | Set\<String\>  | \-       | Paths under which CUGs can be created and will be evaluated. |
| `PARAM_RANKING`             | int            | 200      | Ranking within the composite authorization setup.            |
| | | | |

<a name="pluggability"/>
### Pluggability

_todo: deploying cug-authorization in an oak repository_
_todo: customize cug-authorization by providing custom CugExclude implementation_

##### Examples

_todo_

<!-- references -->