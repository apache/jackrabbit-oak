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

Permissions vs Privileges
--------------------------------------------------------------------------------

### General Notes

Usually it is not required for a application to check the privileges/permissions
of a given session (or set of principals) as this evaluation can be left
to the repository.

For rare cases where the application needs to understand if a given session is 
actually allowed to perform a given action, it is recommend to use `Session.hasPermission(String, String)`
or `JackrabbitSession.hasPermission(String, String...)`

In order to test permissions that are not reflected in the action constants
defined on `Session` or `JackrabbitSession`, the default implementation also allows
to pass the names of the Oak internal permission. 

Alternatively, `AccessControlManager.hasPrivileges(String, Privilege[])` can be used.

The subtle differences between the permission-testing `Session`  and the evaluation
of privileges on `AccessControlManager` are listed below.

### Testing Permissions

#### Variants

- `Session.hasPermission(String absPath, String actions)`
- `Session.checkPermission(String absPath, String actions)`
- `JackrabbitSession.hasPermission(String absPath, @Nonnull String... actions)`

Where

- `absPath` is an absolute path pointing to an existing or non-existing item (node or property)
- `actions` defines a comma-separated string (or string array respectively) of the actions defined on `Session` and `JackrabbitSession` (see below). 
  With the default implementation also Oak internal permission names are allowed ( _Note:_ permission names != privilege names)
  
See section [Permissions](../permission.html#oak_permissions) for a comprehensive
list and the mapping from actions to permissions.

#### Characteristics

- API call always supported even if access control management is not part of the feature set (see corresponding repository descriptor).
- _Note:_ `ACTION_ADD_NODE` is evaluating if the node at the specified absPath can be added; i.e. the path points to the non-existing node you want to add
- Not possible to evaluate custom privileges with this method as those are not respected by the default permission evaluation.
- Restrictions will be respected as possible with the given (limited) information 


### Testing Privileges

#### Variants

- `AccessControlManager.hasPrivileges(String absPath, Privilege[] privileges)`
- `AccessControlManager.getPrivileges(String absPath)`

Where

- `absPath` must point to an existing Node (i.e. existing and accessible to the editing session)
- `privileges` represent an array of supported privileges (see corresponding API calls)

For testing purpose the Jackrabbit extension further allows to verify the privileges 
granted to a given combination of principals, which may or may not reflect the actual 
principal-set assigned to a given `Subject`. These calls (see below) however
requires the ability to read access control content on the target path.

- `JackrabbitAccessControlManager.hasPrivileges(String absPath, Set<Principal> principals, Privilege[] privileges)`
- `JackrabbitAccessControlManager.getPrivileges(String absPath, Set<Principal> principals)`

#### Characteristics

- Only available if access control management is part of the supported feature set of the JCR repository.
- Built-in and/or custom privileges can be tested
- `jcr:addChildNode` evaluates if any child can be added at the parent node identify by the specified absPath. The name of child is not known here! 
- Restrictions may or may not be respected
- Default implementation close to real permission evaluation (not exactly following the specification)

<a name="further_reading"/>
### Further Reading

- [Mapping Privileges to Items](../privilege/mappingtoitems.html)
- [Mapping API Calls to Privileges](../privilege/mappingtoprivileges.html)



