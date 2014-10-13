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

Password Expiry and Force Initial Password Change
--------------------------------------------------------------------------------

### General

Oak provides functionality to expire passwords of users as well as force
users to change their password upon initial (first-time) login.

### Password Expiry

Administrators may configure passwords to expire within a configurable 
amount of time (days). A user whose password has expired will no longer
be able to obtain a session/login.

### Force Initial Password Change

An administrator may configure the system such that a user is forced to
set a new password upon first login. This is a special form of Password 
Expiry above, in that upon creation a user account's password 
is expired by default. Upon initial login, the user will not be able
to obtain a session/login and the password needs to be changed prior
to a next attempt. For specifying the new password, the initial password 
has to be provided.

### Configuration of Expiry / Force Initial Password Change

An administrator may enable password expiry and initial password change 
via the _org.apache.jackrabbit.oak.security.user.UserConfigurationImpl_
OSGi configuration. By default expiry is disabled.

The following configuration options are supported:

- Maximum Password Age (_maxPasswordAge_, days): When greater 0 enables password 
  expiry and sets the expiration time in days.
- Change Password On First Login (_initialPasswordChange_, true|false): 
  When enabled, forces users to change their password upon first login.

### How it works

#### Definition of Expired Password

An expired password is defined as follows:

- The current date-time is after or on the date-time + maxPasswordAge 
  specified in a _rep:passwordLastModified_ property
- OR: Expiry and/or Enforce Password Change is enabled, but no
  _rep:passwordLastModified_ property exists

For the above, a password node _rep:pw_ and a property _rep:passwordLastModified_,
governed by a new _rep:Password_ node type and located in the user's home, have 
been introduced, leaving open future enhancements to password management 
(such as password policies, history, et al):

##### Node Type rep:Password

    [rep:Password]
        - * (UNDEFINED) protected
        - * (UNDEFINED) protected multiple

##### Node rep:passwords and Property rep:passwordLastModified

    [rep:User]  > rep:Authorizable, rep:Impersonatable
        + rep:pw (rep:Password) = rep:Password protected
        ...
        
The _rep:pw_ node and the _rep:passwordLastModified_ property are defined
protected in order to guard against the user modifying (overcoming) her 
password expiry. The new sub-node also has the advantage of allowing repository 
consumers to e.g. register specific commit hooks / actions on such a node.

In the future the _rep:password_ property on the user node may be migrated 
to the _rep:pw_ sub-node.

#### User Creation With Default Expired Password

Upon initial creation of a user, the _rep:passwordLastModified_ property is
omitted. If expiry or _initialPasswordChange_ are enabled, the absence of the
property will be interpreted as immediate expiry of the password. When
subsequently the user changes her password via _User#changePassword_, the
_rep:passwordLastModified_ property is set and henceforth interpreted.

#### Core Authentication Password Expiry Aware

A login module must throw a _javax.security.auth.login.CredentialExpiredException_
upon encountering an expired password. A consumer implementation can then 
differentiate between a failed login (due to a wrong password specified) and an
expired password, allowing the consumer to take action, e.g. to redirect to a
change password form.

In Oak, the Authentication (currently _UserAuthentication_) implementation
compares within its _#authenticate()_ method the system time with the value
stored in the rep:passwordLastModified_ and throws a _CredentialExpiredException_
if now is after or on the date-time specified by the value.

In the case of _initialPasswordChange_ a password is considered expired if no
_rep:passwordLastModified_ property can be found on login.

Both expiry and force initial password change are checked *after* regular 
credentials verification, so as to prevent an attacker identifying valid users
by being redirected to a change password form upon expiry.

#### Oak JCR XML Import

When users are imported via the Oak JCR XML importer, the expiry relevant
nodes and property are supported. If the XML specifies a _rep:pw_ node and
optionally a _rep:passwordLastModified_ property, these are imported, irrespective
of the password expiry or force initial password change being enabled in the
configuration. If they're enabled, the imported property will be used in the
normal login process as described above. If not enabled, the imported property
will have no effect.

On the other hand, if the imported user already exists, potentially existing 
_rep:passwordLastModified_ properties will be overwritten with the value from
the import. If password expiry is enabled, this may cause passwords to expire
earlier or later than anticipated, governed by the new value. Also, an import
may create such a property where none previously existed, thus effectively
cancelling the need to change the password on first login - if the feature
is enabled.

Therefore customers using the importer in such fashion should be aware of the
potential need to enable password expiry/force initial password change for the
imported data to make sense, and/or the effect on already existing/overwritten
data.

#### Changing an Expired Password

Oak supports changing a user's expired password as part of the normal login
process.

Consumers of the repository already specify _javax.jcr.SimpleCredentials_ during
login, as part of the normal authentication process. In order to change the
password for an expired user, the login may be called with the affected user's
_javax.jcr.SimpleCredentials_, while additionally providing the new password
via a credentials attribute _newPassword_.

After verifying the user's credentials, *before* checking expiry, said attribute
is then used by the _UserAuthentication_ to change the user's password.

This way the user can change the password while the expiry check succeeds
(password expired = false) and a session/login is provided at the same time.

This method of changing password via the normal login call only works if a
user's password is in fact expired and cannot be used for regular password
changes (attribute is ignored, use _User#changePassword_ directly instead).
