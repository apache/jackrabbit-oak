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

Since version 1.1.0 Oak provides functionality to expire passwords of users as 
well as force users to change their password upon initial (first-time) login.

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

### Configuration

An administrator may enable password expiry and initial password change 
via the `org.apache.jackrabbit.oak.security.user.UserConfigurationImpl`
OSGi configuration. By default both features are disabled.

The following configuration options are supported:

| Parameter                       | Type    | Default  | Description            |
|-------------------------------------      |-------- -|------------------------|
| `PARAM_PASSWORD_MAX_AGE`        | int     | 0        | Number of days until the password expires. |
| `PARAM_PASSWORD_INITIAL_CHANGE` | boolean | false    | boolean flag to enable initial pw change.  |

Note:

- Maximum Password Age (`maxPasswordAge`) will only be enabled when a value greater 0 is set (expiration time in days).
- Change Password On First Login (`initialPasswordChange`): When enabled, forces users to change their password upon first login.

### How it works

#### Definition of Expired Password

An expired password is defined as follows:

- The current date-time is after or on the date-time + maxPasswordAge 
  specified in a `rep:passwordLastModified` property
- OR: Expiry and/or Enforce Password Change is enabled, but no
  `rep:passwordLastModified` property exists

For the above, a password node `rep:pw` and a property `rep:passwordLastModified`,
governed by a new `rep:Password` node type and located in the user's home, have 
been introduced, leaving open future enhancements to password management 
(such as password policies, history, et al):

#### Representation in the Repository

##### Node Type rep:Password

    [rep:Password]
        - * (UNDEFINED) protected
        - * (UNDEFINED) protected multiple

##### Node rep:pwd and Property rep:passwordLastModified

    [rep:User]  > rep:Authorizable, rep:Impersonatable
        + rep:pwd (rep:Password) = rep:Password protected
        ...
        
The `rep:pw` node and the `rep:passwordLastModified` property are defined
protected in order to guard against the user modifying (overcoming) her 
password expiry. The new sub-node also has the advantage of allowing repository 
consumers to e.g. register specific commit hooks / actions on such a node.

In the future the `rep:password` property on the user node may be migrated 
to the `rep:pw` sub-node.

#### User Creation

Upon initial creation of a user, the `rep:passwordLastModified` property is
omitted. If expiry or `initialPasswordChange` are enabled, the absence of the
property will be interpreted as immediate expiry of the password. When
subsequently the user changes her password via `User#changePassword`, the
`rep:passwordLastModified` property is set and henceforth interpreted.

#### Authentication 

A login module must throw a `javax.security.auth.login.CredentialExpiredException`
upon encountering an expired password. A consumer implementation can then 
differentiate between a failed login (due to a wrong password specified) and an
expired password, allowing the consumer to take action, e.g. to redirect to a
change password form.

In Oak, the [Authentication] implementation provided by default with the user
management compares within its `authenticate()` method the system time with the value
stored in the `rep:passwordLastModified` and throws a [CredentialExpiredException]
if now is after or on the date-time specified by the value.

In the case of `initialPasswordChange` a password is considered expired if no
`rep:passwordLastModified` property can be found on login.

Both expiry and force initial password change are checked *after* regular 
credentials verification, so as to prevent an attacker identifying valid users
by being redirected to a change password form upon expiry.

##### UserAuthenticationFactory

As described with section [User Management: The Default Implementation](default.html#pluggability) 
it is possible to change the default implementation of the `UserAuthenticationFactory`
by pluggin a custom implementation at runtime.

It's important to note that the authentication related part of password expiry is 
handled by the [Authentication] implementation exposed by the default [UserAuthenticationFactory]. 
Replacing the factory will ultimately disable the password expiry feature
unless a custom implementation respects and enforces the constraints explained
before.

#### Changing an Expired Password

Oak supports changing a user's expired password as part of the normal login
process.

Consumers of the repository already specify `javax.jcr.SimpleCredentials` during
login, as part of the normal authentication process. In order to change the
password for an expired user, the login may be called with the affected user's
[SimpleCredentials], while additionally providing the new password
via a credentials attribute `newPassword`.

After verifying the user's credentials, *before* checking expiry, said attribute
is then used by the `Authentication` implementation to change the user's password.

This way the user can change the password while the expiry check succeeds
(password expired = false) and a session/login is provided at the same time.

This method of changing password via the normal login call only works if a
user's password is in fact expired and cannot be used for regular password
changes (attribute is ignored, use `User#changePassword` directly instead).

Should the [Password History feature](history.html) be enabled, and - for the
above password change - a password already in the history be used, the change
will fail and the login still throw a [CredentialExpiredException]. In order
for consumers of the exception to become aware that the credentials are
still considered expired, and that the password was not changed due to the 
new password having been found in the password history, the credentials object
is fitted with an additional attribute with name `PasswordHistoryException`.

This attribute may contain the following two values:

- _"New password was found in password history."_ or 
- _""New password is identical to the current password."_

#### XML Import

When users are imported via the Oak JCR XML importer, the expiry relevant
nodes and property are supported. If the XML specifies a `rep:pw` node and
optionally a `rep:passwordLastModified` property, these are imported, irrespective
of the password expiry or force initial password change being enabled in the
configuration. If they're enabled, the imported property will be used in the
normal login process as described above. If not enabled, the imported property
will have no effect.

On the other hand, if the imported user already exists, potentially existing 
`rep:passwordLastModified` properties will be overwritten with the value from
the import. If password expiry is enabled, this may cause passwords to expire
earlier or later than anticipated, governed by the new value. Also, an import
may create such a property where none previously existed, thus effectively
cancelling the need to change the password on first login - if the feature
is enabled.

Therefore customers using the importer in such fashion should be aware of the
potential need to enable password expiry/force initial password change for the
imported data to make sense, and/or the effect on already existing/overwritten
data.

<!-- hidden references -->
[SimpleCredentials]: http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/SimpleCredentials.html
[CredentialExpiredException]: https://docs.oracle.com/javase/7/docs/api/javax/security/auth/login/CredentialExpiredException.html
[UserAuthenticationFactory]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/user/UserAuthenticationFactory.html
[Authentication]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/Authentication.html
