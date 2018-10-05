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

Caching Results of Principal Resolution
--------------------------------------------------------------------------------

### General

Since Oak 1.3.4 this `UserPrincipalProvider` optionally allows for temporary
caching of the principal resolution mainly to optimize login performance (OAK-3003).

This cache contains the result of the group principal resolution as performed by
`PrincipalProvider.getPrincipals(String userId)`and `PrincipalProvider.getGroupMembership(Principal)`
and will read from the cache upon subsequent calls for the configured expiration
time.

### Configuration

An administrator may enable the group principal caching via the
_org.apache.jackrabbit.oak.security.user.UserConfigurationImpl_
OSGi configuration. By default caching is disabled.

The following configuration option is supported:

- Cache Expiration (`cacheExpiration`): Specifying a long greater 0 enables the
  caching.

NOTE: It is important that the configured expiration time balances between login
performance and cache invalidation to reflect changes made to the group membership.
An application that makes use of this cache, must be able to live with shot term
diverging of principal resolution and user management upon repository login.

It is expected that the cache is used in scenarios where subsequent repository
login calls can (or even should) result in the creation of a `javax.security.auth.Subject`
with equal principal set irrespective of group membership changes.
See section Invalidation below for further details.


### How it works

#### Caching Principal Names

If the feature is enabled, evaluating `UserPrincipalProvider.getPrincipals(String userId)`
and `PrincipalProvider.getGroupMembership(Principal)` as well as the corresponding
calls on `PrincipalManager` will trigger the group principal names to be remembered
in a cache if the following conditions are met:

- a valid expiration time is configured (i.e. > 0),
- the `PrincipalProvider` has been obtained for a system session (see below),
- the tree to hold the cache belongs to a user (i.e. tree with primary type
  `rep:User` (i.e. no caches are created for groups)

The cache itself consists of a tree named `rep:cache` with the built-in node type
`rep:Cache`, which defines a mandatory, protected `rep:expiration` property and
may have additional protected, residual properties.

Subsequent calls will read the names of the group principals from the cache until
the cache expires. Once expired the default resolution will be performed again in
order to update the cache.

##### Limitation to System Calls

The creation and maintenance of this caches as well as the shortcut upon reading
is limited to system internal sessions for security reasons: The cache must always
be filled with the comprehensive list of group principals (as required upon login)
as must any subsequent call never expose principal information that might not
be accessible in the non-cache scenario where access to principals is protected
by regular permission evalution.

<a name="validation"/>
##### Validation

The cache is system maintained, protected repository content that can only
be created and updated by the implementation. Any attempt to manipulate these
caches using JCR or Oak API calls will fail. Also the cache can only be created
or updated using the internal system subject.

Also this validation is always enforce irrespective on whether the caching
feature is enabled or not, to prevent unintended manipulation.

These constraints and the consistency of the cache structure is asserted by a
dedicated `CacheValidator`. The corresponding errors are all of type `Constraint`
with the following codes:

| Code              | Message                                                  |
|-------------------|----------------------------------------------------------|
| 0034              | Attempt to create or change the system maintained cache. |

Note however, that the cache tree might be removed by any session that has
sufficient privileges to remove it.


##### Cache Invalidation

The caches hold with the different user trees get invalidated once the expiration
time is reached. There is no explicit, forced invalidation if group membership
as reflected by the user management implementation is being changed.

Consequently, system sessions which might read principal information from the cache
(if enabled) can be provided with a set of principals (as stored in the cache)
that might have diverged from the group membership stored in the repository
for the time until the cache expires.

Applications that rely on principal resolution being _always_ in sync with the
revision associated with the system session that perform the repository login,
must not enable the cache.

Similarly, applications that have due to their design have an extremely high
turnover wrt group membership might not be able to profit from this cache in
the expected way.


#### Interaction With User Management

The cache is created and maintained by the `PrincipalProvider` implementation as
exposed by the optional `UserConfiguration.getUserPrincipalProvider` call and
will therefore only effect the results provided by the principal management API.

Regular Jackrabbit user management API calls are not affected by this cache and
vice versa; i.e. changes made using the user management API have no immediate
effect on the cache and will not trigger it's invalidation.

In other words user management API calls will always read from the revision of the
content repository that is associated with the give JCR `Session` (and Oak
`ContentSession`). The same is true for principal management API calls of all
non-system sessions.

See the introduction and section Invalidation above for the expected behavior
for system sessions.

##### XML Import

When users are imported via JCR XML import, the protected cache structure will
be ignored (i.e. will not be imported).
