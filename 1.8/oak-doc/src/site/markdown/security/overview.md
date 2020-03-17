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

The Oak Security Layer
--------------------------------------------------------------------------------

### General

 * [Introduction to Oak Security](introduction.html)

### Authentication

 * [Overview](authentication.html)
 * [Differences wrt Jackrabbit 2.x](authentication/differences.html)
 * [Authentication : Implementation Details](authentication/default.html)
 * [Pre-Authentication](authentication/preauthentication.html)
 
#### Extensions
 
 * [Token Authentication and Token Management](authentication/tokenmanagement.html)
 * [External Authentication](authentication/externalloginmodule.html)
     * [User and Group Synchronization](authentication/usersync.html)
     * [Identity Management](authentication/identitymanagement.html)
     * [LDAP Integration](authentication/ldap.html)

### Authorization

 * [Overview](authorization.html)
     * [Access Control Management](accesscontrol.html)
     * [Permission Evalution](permission.html)
     * [Combining Multiple Authorization Models](authorization/composite.html)
  
#### Access Control Management

 * [Overview](accesscontrol.html)
 * [Differences wrt Jackrabbit 2.x](accesscontrol/differences.html)
 * [Access Control Management : The Default Implementation](accesscontrol/default.html)
 * [Using the API](accesscontrol/editing.html)

#### Permissions

 * [Overview](permission.html)
    * [Permissions vs Privileges](permission/permissionsandprivileges.html)
 * [Differences wrt Jackrabbit 2.x](permission/differences.html)
 * [Permissions : The Default Implementation](permission/default.html)
    * [Permission Evaluation in Detail](permission/evaluation.html)
    
#### Privilege Management

 * [Overview](privilege.html)
 * [Differences wrt Jackrabbit 2.x](privilege/differences.html)
 * [Privilege Management : The Default Implementation](privilege/default.html)
 * Mapping Privileges to Items and API Calls
    * [Mapping Privileges to Items](privilege/mappingtoitems.html)
    * [Mapping API Calls to Privileges](privilege/mappingtoprivileges.html)

#### Extensions

 * [Restriction Management](authorization/restriction.html)
 * [Managing Access with Closed User Groups (CUG)](authorization/cug.html)

### Principal Management

 * [Overview](principal.html)
 * [Differences wrt Jackrabbit 2.x](principal/differences.html)

### User Management

 * [Overview](user.html)
 * [Differences wrt Jackrabbit 2.x](user/differences.html)
 * [User Management : The Default Implementation](user/default.html)
    * [Group Membership](user/membership.html)
    * [Authorizable Actions](user/authorizableaction.html)
    * [Group Actions](user/groupaction.html)
    * [Authorizable Node Name Generation](user/authorizablenodename.html)
    * [Password Expiry and Force Initial Password Change](user/expiry.html)
    * [Password History](user/history.html) 
 * [Searching Users and Groups](user/query.html)
