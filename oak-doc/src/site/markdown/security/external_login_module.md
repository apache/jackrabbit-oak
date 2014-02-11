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
======================

External Login Module
------------------------------------------------------------------------------------------------------------------------

### Overview
The purpose of the external login module is to provide a base implementation that allows easy integration of 3rd party 
authentication and identity systems, such as LDAP. The general mode of the external login module is to use the external
system as authentication source and as a provider for users and groups.

what it does:

* facilitate the use of a 3rd party system for authentication
* simplify populating the oak user manager with identities from a 3rd party system

what it does not:

* provide a transparent oak user manager
* provide a transparent oak principal provider.
* offer services for background synchronization of users and groups

### Types of login modules
In order to understand how login modules work and how Oak can help providing extension points we need to look at how
JAAS authentication works in general and discuss where the actual credential-verification is performed.

#### Brief recap of the JAAS authentication
The following section is copied and adapted from the javadoc of [javax.security.auth.spi.LoginModule]:

The authentication process within the `LoginModule` proceeds in two distinct phases. 

1. 1. In the first phase, the `LoginModule`'s `login` method gets invoked by the `LoginContext`'s `login` method.
   2. The `login` method for the `LoginModule` then performs the actual authentication (prompt for and verify a 
      password for example) and saves its authentication status as private state information. 
   3. Once finished, the `LoginModule`'s login method either returns `true` (if it succeeded) or `false` (if it should 
      be ignored), or throws a `LoginException` to specify a failure. In the failure case, the `LoginModule` must not 
      retry the authentication or introduce delays. The responsibility of such tasks belongs to the application. 
      If the application attempts to retry the authentication, the `LoginModule`'s `login` method will be called again.

2. 1. In the second phase, if the `LoginContext`'s overall authentication succeeded (the relevant REQUIRED, REQUISITE, 
      SUFFICIENT and OPTIONAL LoginModules succeeded), then the `commit` method for the `LoginModule` gets invoked. 
   2. The `commit` method for a `LoginModule` checks its privately saved state to see if its own authentication 
      succeeded. 
   3. If the overall `LoginContext` authentication succeeded and the `LoginModule`'s own authentication succeeded, then
      the `commit` method associates the relevant Principals (authenticated identities) and Credentials (authentication 
      data such as cryptographic keys) with the Subject located within the `LoginModule`.
   4. If the `LoginContext`'s overall authentication failed (the relevant REQUIRED, REQUISITE, SUFFICIENT and OPTIONAL 
      LoginModules did not succeed), then the `abort` method for each `LoginModule` gets invoked. In this case, the 
      `LoginModule` removes/destroys any authentication state originally saved.
      
#### Login module execution order
Very simply put, all the login modules that participate in JAAS authentication are configured in a list and can have
flags indicating how to treat their behaviors on the `login()` calls.

JAAS defines the following module flags:  
(The following section is copied and adapted from the javadoc of [javax.security.auth.login.Configuration])

1. **Required**
:  The LoginModule is required to succeed.  
   If it succeeds or fails, authentication still continues to proceed down the LoginModule list.

2. **Requisite**
:  The LoginModule is required to succeed.  
   If it succeeds, authentication continues down the LoginModule list.
   If it fails, control immediately returns to the application (authentication does not proceed down the LoginModule 
   list).

3. **Sufficient**
:  The LoginModule is not required to succeed.    
   If it does succeed, control immediately returns to the application (authentication does not proceed down the 
   LoginModule list).
   If it fails, authentication continues down the LoginModule list.

4. **Optional**
:  The LoginModule is not required to succeed.  
   If it succeeds or fails, authentication still continues to proceed down the LoginModule list.
 
The overall authentication succeeds **only** if **all** Required and Requisite LoginModules succeed. If a Sufficient 
LoginModule is configured and succeeds, then only the Required and Requisite LoginModules prior to that Sufficient 
LoginModule need to have succeeded for the overall authentication to succeed. If no Required or Requisite LoginModules 
are configured for an application, then at least one Sufficient or Optional LoginModule must succeed.

#### Authentication and subject population
The goal of the external login module is to provide a very simple way of using 
_"the users stored in an external system for authentication and authorization in the Oak content repository"_. So the
easiest way of doing this is to import the users on-demand when they log in. 

#### Password caching
In order to prevent extensive authentication calls against the 3rd party system the user's credentials, in 
particular the passwords need to be cached. There are two different way doing this.

The first way is to only cache the password (encrypted) in memory and expire them over time. this has the advantage 
that they are not copied to the local repository and can be invalidated easier and with a different cycle than the
actual user synchronization. It has the disadvantage, that users won't be able to login, if the 3rd party system is
offline.

The alternative is to cache the passwords in the repository together with the synced user.
this has the advantage that the 3rd party system can be offline and users will
still be able to login. It has the following disadvantages:

- password are copied to the local system and stored with the users in a encrypted form. This might be a security concern and might not comply with security policies.
- it only works for simple password based credentials.

### Behavior of the External Login Module

#### General
The external login module has 2 main tasks. one is to authenticate credentials against a 3rd party system, the other is
to coordinate syncing of the respective users and groups with the JCR repository (via the UserManager).

If a user needs re-authentication (for example, if the cache validity expired or if the user is not yet present in the
local system at all), the login module must check the credentials with the external system during the `login()` method. 
If authentication succeeds, it must return `true` or throw a `LoginException` if authentication failed.  

There are some nuances to this and it is important to understand the exact behavior of the login module(s) so that the JAAS login can be configured correctly:

**LoginModuleImpl**

The behavior of the default login module is relatively simple, so it is explained first:

upon login():

* if a user does not exist in the repository (i.e. cannot be provided by the user manager) it **returns `false`**.
* if a user exists in the repository and the credentials don't match, it **throws `LoginException`**
* if a user exists in the repository and the credentials match, it **returns `true`** 
   * also, it adds the credentials to the shared state
   * also, it adds the login name to the shared state 
   * also, it calculates the principals and adds them to the private state
   * also, it adds the credentials to the private state

upon commit():

* if the private state contains the credentials and principals, it adds them (both) to the subject and **returns `true`**
* if the private state does not contain credentials and principals, it clears the state and **returns `false`**


**ExternalLoginModule**

Note:

* users (and groups) that are synced from the 3rd party system contain a `rep:syncSource` (TBD) property. This allows to identify the external users and distinguish them from others.
* to reduce expensive syncing, the synced users and groups have sync timestamp `rep:lastSynced` and are considered valid for a configurable time. if they expire, they need to be validated against the 3rd party system again.

upon login():

* if the passwords are cached in memory and the credentials are valid, it puts the credentials in the private state and  **returns `true`**
* if the user exists in the 3rd party system and the credentials match, it puts the credentials in the private state and **returns `true`**
* if the user exists in the 3rd party system but the credentials don't match it **throws `LoginException`**
* if the user does not exist in the 3rd party system, it **returns `false`**

upon commit():

* if there is no credentials in the private state, it **returns `false`**
* if there are credentials in the private state, possibly sync the user and then propagate the subject and **return `true`**

<!-- 
* If a user does **not** exist in the 3rd party system, it could be that
    1. the user was not and will never exist there, and authentication should proceed to the next login module.
       This is for example the case for the *admin* user that only exists locally.
    2. the user was removed from the 3rd party system but still exists locally. In this case, the user needs to be
       removed locally and authentication needs to proceed to the next login module
  
  So in both cases above, the external login module needs to returns `false` in its `login()` method.
  
* If a user **does** exist in the 3rd party system, it could be that
    1. no local user with the same userid exists, so the result of the 3rd party authentication is sufficient enough.
    2. a local user with the same userid exists (who is not an imported 3rd party user) then it depends on the
       configuration if to rely on the external authentication or not. This problem is especially important to note, 
       if the external and local users cannot be distinguished by principal name (which is usually the case).  
       A good default is to decline authentication (i.e. return `false`) and rely on the default login module in such
       cases.
       
Any `ExternalLoginModule` needs to be configured as **sufficient** and listed **before** the default `LoginModuleImpl`. 
this will lead to the following behaviors:

1. if a (remote) user does not exist in the repository, he is authenticated against the 3rd party system and...
    * if the user does not exist in the 3rd party system, the `login()` returns `false` and the JAAS authentication 
      continues with the next login module.
    * if authentication fails, the `login()` throws a `LoginException` and the overall JAAS authentication continues
      with the next module. Note that this will allow local user override that also exist in the external system.
    * if authentication succeeds, the 1. phase of the JAAS authentication is completed. There are extra steps that
      the external login module needs to perform in order to sync the user.
2. if a (remote) user exists in the repository and he does not need re-authentication, the `login()` method returns
   `true` and authentication completes. 
3. if a (remote) user exists in the repository and he needs re-authentication then the user is removed from the
   repository and the same steps as in 1. above apply.
   
Since the module is configured as **sufficient** any successful authentication against the 3rd party system will
complete the first phase of the JAAS authentication and the `ExternalLoginModule` has time to actually sync the
user during the 2nd phase in the `commit()` method. But this depends again on the mode of operation. see pseudo
code sections below.
-->

#### User Synchronization
TODO

#### Groups Synchronization
TODO

### Configuration

#### JAAS Configuration
The external login module as usually configured as **sufficient** and **before** the `LoginModuleImpl`.

other config

* password caching expiration time (0 == disable password caching)
* maximum passwords to cache (i.e. num users)
* synced user expiration
* synced group expiration
* synced membership expiration
* sync groups
* user intermediate path / split dn
* group intermediate path / split dn
* user sync modification attribute
* group sync modification attribute
* autoGroupMembership (user / groups)
* userAttributeMapping
* groupAttributeMapping
* ldap user root (dn)
* ldap group root (dn)
* ldap userIdAttribute
* ldap userFilter
* ldap groupFilter
* ldap groupNameAttribute
* ldap groupMembershipAttribute
* ldap searchTimeout
* groupNestingDepth

* local/external user precedence. default is: local wins. best use include/exclude of user patterns. e.g. admin 
  should never be external.
* password sync or not.
* various cache expiration times (pwd, users, groups)
* 

#### pseudo code for users with synced passwords
````
login() {
    extract userid and password from credentials    
    find user in repository
    if (user exists in repository) {
        if (user is local user) {
            // not our user - decline.
            return false;
        }
        if (user needs sync) {
            flag user to be synced
        } else {
            // delegate work to default login module
            return false;    
        }
    }    
    find user in remote system
    if (user exists in remote system) {
        authenticate user against remote system
        if (authentication successful) {
            sync user including the provided password locally
            // delegate further work to default login module
            return false;
        } else {
            throw LoginException();
        }
    } else {
        if (user is flagged) {
            remove user
        }
        return false;
    }
}
commit() {
    // nothing to do
    return false;
}
````

#### pseudo code for users without synced passwords

````
login() {
    extract userid and password from credentials
    find user in repository
    if (user exists in repository) {
        if (user is local user) {
            // not our user - decline.
            return false;    
        }
        if (user needs sync) {
            flag user to be synced
        } else {
            check if users credentials are cached and not expired
            if (cached credentials match) {
                // authentication successful
                return true;
            }
            // here it could be that the user changed his password on the IDP
            // and he already provided the new password in the login. so we can't
            // throw the LoginException here but need to revalidate him against the IDP.
        }
        clear cached credentials
    }
    find user in remote system
    if (user exists in remote system) {
        authenticate user against remote system
        if (authentication successful) {
            flag user to be  synced
            cache credentials
            return true;
        } else {
            throw LoginException();
        }
    } else {
        if (user is flagged) {
            remove user
        }
        return false;
    }
}
commit() {
    if (user needs to be synced ) {
        sync user
    }
    if (this module did authenticate the user) {
        populate subject with the correct principals
        return true;
    } else {
        return false;
    }
}
````



<!-- references -->
[javax.security.auth.spi.LoginModule]: http://docs.oracle.com/javase/6/docs/api/javax/security/auth/spi/LoginModule.html
[javax.security.auth.login.Configuration]: http://docs.oracle.com/javase/6/docs/api/javax/security/auth/login/Configuration.html





