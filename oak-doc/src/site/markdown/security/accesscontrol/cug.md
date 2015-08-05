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

### CUG API

_todo_

### Characteristics of the CUG Implementation

_todo_

#### CUG Representation in the Repository

##### Content Structure

_todo_

<a name="validation"/>
##### Validation

The consistency of this content structure both on creation and modification is
asserted by a dedicated `TokenValidator`. The corresponding error are
all of type `AccessControl` with the following codes:

| Code              | Message                                                  |
|-------------------|----------------------------------------------------------|
| 0020              | Attempt to change primary type of/to cug policy          |
| 0021              | Wrong primary type of 'rep:cugPolicy' node               |
| 0022              | Access controlled not not of mixin 'rep:CugMixin'        |
| 0023              | Wrong name of node with primary type 'rep:CugPolicy'     |

### Configuration

_todo_

#### Configuration Parameters

_todo_


#### Examples

_todo_

### Pluggability

_todo_

##### Examples

_todo_

<!-- references -->