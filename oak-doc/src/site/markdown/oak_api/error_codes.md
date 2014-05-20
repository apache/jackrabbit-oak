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

Error Codes
================================================================================

Since [OAK-764] the CommitFailedExceptions thrown by commit hooks in Oak come
with error codes that help identify the cause of a problem and locate additional
information about the issue. This page is an informal registry of common error codes.

### Type Constraint

#### Node type validation

| OakConstraint000x | Primary and mixin type information |
|-------------------|------------------------------------|
| 0001              | The primary type X does not exist  |
| 0002              | Mixin type X used as the primary type |
| 0003              | Abstract type X used as the primary type |
| 0004              | No default primary type available for child node X |
| 0005              | The mixin type X does not exist |
| 0006              | Primary type X used as a mixin type |
| 0007              | Abstract type X used as a mixin type |

| OakConstraint002x | Presence of mandatory items                     |
|-------------------|-------------------------------------------------|
| 0021              | Mandatory property X not included in a new node |
| 0022              | Mandatory property X can not be removed |
| 0025              | Mandatory child node X not included in a new node |
| 0026              | Mandatory child node X can not be removed |

#### Privilege Validation

| Code              | Message                                         |
|-------------------|-------------------------------------------------|
| 0041              | Modification of existing privilege definition X |
| 0042              | Un-register privilege X  |
| 0043              | Next bits not updated |
| 0044              | Privilege store not initialized |
| 0047              | Invalid declared aggregate name X |
| 0048              | PrivilegeBits are missing |
| 0049              | PrivilegeBits already in used |
| 0050              | Singular aggregation is equivalent to existing privilege. |
| 0051              | Declared aggregate X is not a registered privilege |
| 0052              | Detected circular aggregation |
| 0053              | Custom aggregate privilege X is already covered. |

#### User Validation

_todo_

### Type Access

#### Permission Validation

_todo_


<!-- hidden references -->
[OAK-764]: https://issues.apache.org/jira/browse/OAK-764