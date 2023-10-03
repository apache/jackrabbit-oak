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
### Privilege Management : Differences wrt Jackrabbit 2.x

#### Registration of Custom Privileges
As far as registration of custom privileges the Oak implementation behaves
different to Jackrabbit 2.x in the following two aspects:

- Registration of new privileges fails with `IllegalStateException` if the editing session has pending changes.
- Any validation is performed by CommitHooks in order to make sure that modifications made on the Oak API directly is equally verified. Subsequently any violation (permission, privilege consistency) is only detected at the end of the registration process. The privilege manager itself does not perform any validation.

#### Built-in Privilege Definitions
The following changes have been made to built-in privilege definitions:

- Modifications:
    - `jcr:read` is now an aggregation of `rep:readNodes` and `rep:readProperties`
    - `jcr:modifyProperties` is now an aggregation of `rep:addProperties`, `rep:alterProperties` and `rep:removeProperties`
- New Privileges defined by Oak 1.0:
    - `rep:userManagement`
    - `rep:readNodes`
    - `rep:readProperties`
    - `rep:addProperties`
    - `rep:alterProperties`
    - `rep:removeProperties`
    - `rep:indexDefinitionManagement`