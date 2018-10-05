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

Group Membership
--------------------------------------------------------------------------------

### Jackrabbit API

The Jackrabbit API extensions provide various methods to edit and explore the
member relationship of users and groups:

- [org.apache.jackrabbit.api.security.user.Group]
    - `getDeclaredMembers() Iterator<Authorizable>`
    - `getMembers() Iterator<Authorizable>`
    - `isDeclaredMember(Authorizable) boolean`
    - `isMember(Authorizable boolean`
    - `addMember(Authorizable) boolean`
    - `removeMember(Authorizable) boolen`
    - `addMembers(String...) Set<String>`
    - `removeMembers(String...) Set<String>`

- [org.apache.jackrabbit.api.security.user.Authorizable]
    - `declaredMemberOf() Iterator<Group>`
    - `memberOf() Iterator<Group>`

### Characteristics of the Default Implementation

#### Member Representation in the Repository

##### Behavior in Jackrabbit 2.x
With the default configuration Jackrabbit 2.x stores the group members as
_weak references_ in a `rep:members` multi value property in the group node.
If the `groupMembershipSplitSize` configuration parameter is set and valid,
the group memberships are collected in a node structure below `rep:members` instead
of the default multi valued property. Its value determines the maximum number of
member properties until additional intermediate nodes are inserted. Valid parameter
values are integers > 4. The node structure is a balanced b-tree where only the
leave nodes carry the actual values in residual properties which name is the
principal name of the member.

##### Behavior as of OAK 1.0
As of Oak the user manager automatically chooses an appropriate storage structure
depending on the number of group members. If the number of members is low they
are stored as _weak references_ in a `rep:members` multi value property. This is
similar to Jackrabbit 2.x. If the number of members is high the user manager
will limit the size of the multi value properties and create overflow
`rep:MemberReferences` nodes below a `rep:membersList` node to hold the extra members.

##### Relevant new and modified node types
    [rep:Group] > rep:Authorizable, rep:MemberReferences
      + rep:members (rep:Members) = rep:Members multiple protected VERSION /* @deprecated */
      + rep:membersList (rep:MemberReferencesList) = rep:MemberReferencesList protected COPY

    [rep:MemberReferences]
      - rep:members (WEAKREFERENCE) protected multiple < 'rep:Authorizable'

    [rep:MemberReferencesList]
      + * (rep:MemberReferences) = rep:MemberReferences protected COPY

##### Examples

###### Example Group with few members
*(irrelevant properties excluded)*

    {
        "jcr:primaryType": "rep:Group",
        "rep:principalName": "contributor",
        "rep:members": [
            "429bbd5b-46a6-3c3d-808b-5fd4219d5c4d",
            "ca58c408-fe06-357e-953c-2d23ffe1e096",
            "3ebb1c04-76dd-317e-a9ee-5164182bc390",
            "d3c827d3-4db2-30cc-9c41-0ed8117dbaff",
            "f5777a0b-a933-3b4d-9405-613d8bc39cc7",
            "fdd1547a-b19a-3154-90da-1eae8c2c3504",
            "65c3084e-abfc-3719-8223-72c6cb9a3d6f"
        ]
    }

###### Example Group with many members
*(irrelevant properties excluded)*

    {
        "jcr:primaryType": "rep:Group",
        "rep:principalName": "employees",
        "rep:membersList": {
            "jcr:primaryType": "rep:MemberReferencesList",
            "0": {
                "jcr:primaryType": "rep:MemberReferences",
                "rep:members": [
                    "429bbd5b-46a6-3c3d-808b-5fd4219d5c4d",
                    "ca58c408-fe06-357e-953c-2d23ffe1e096",
                    ...
                ]
            },
            ...
            "341": {
                "jcr:primaryType": "rep:MemberReferences",
                "rep:members": [
                    "fdd1547a-b19a-3154-90da-1eae8c2c3504",
                    "65c3084e-abfc-3719-8223-72c6cb9a3d6f",
                    ...
                ]
            }
        }
    }

*Note*: The exact threshold value that determines the storage strategy is an
implementation detail and might even vary depending on the underlying persistence layer.
In Oak 1.0 the threshold value is set to 100.

#### Upgrading Groups from Jackrabbit 2.x to Oak content structure

Upon upgrade from a Jackrabbit 2.x repository to OAK the group member lists that
adjusted to reflect the new content structure as created by the OAK user management
implementation; this means that the group member node structure will be changed
by the migration process. Applications that rely on these implementation
details of Jackrabbit 2.x user management instead of use the corresponding
API calls will need to be modified accordingly.

#### XML Import of Group Members

Importing group members through the import methods in `javax.jcr.Session` or
`javax.jcr.Workspace` is storage agnostic and supports both, property based and
node based, strategies and is backward compatible to content exported from
Jackrabbit 2.x. The group member lists that are modified during an import are
internally processed using the normal user manager APIs. This implies that the
node structure after the import might not be the same as the one represented in
the input.

#### Add and Remove Group Members by Id

Since Oak 1.3.4 the default user management implementation also allows to modify
group membership by specifying the member id(s) (see [JCR-3880] and [OAK-3170]).
The following details are worth mentioning:

- a `null` or empty String id will immediately fail the operation with `ConstraintViolationException`; changes already made will not be reverted,
- an attempt to make the same group member of itself will list that id in the return value but will not fail the operation,
- duplicate ids in the parameter list will be silently ignored,
- <s>cyclic membership validation is postponed to the validator called upon `Root.commit`
  and will only fail at that point; the cyclic membership then needs to be manually
  resolved by the application</s> (see [OAK-3170] and below)
- whether or not a non-existing (or not accessible) authorizable can be added or
  removed depends on the configured `ImportBehavior`:
    - ABORT: each id is resolved to the corresponding authorizable; if it doesn't
      exist `ConstraintViolationException` is thrown immediately; changes already
      made will not be reverted.
    - BESTEFFORT: the specified ids are not resolved to the corresponding
      authorizables and are silently added|removed to|from the set of members;
      ids that were not successfully added|removed are listed in the return value.
    - IGNORE: each id is resolved to the corresponding authorizable; if it doesn't
      exist it will be returned as _failed_ in the return value.

#### Invalid Membership

##### Invalid Authorizable

Adding a different implementation of `Authorizable` is not allowed. This is always 
verified when calling `Group.addMember(Authorizable)`.

##### Same Group as Member

Adding the target group as member of itself will not succeed. When adding 
members by ID (`Group.addMembers(String...)`) the violation is spotted by 
simple ID comparison.

##### Everyone Group and Everyone as Member

The group representing the `EveryonePrincipal` is specially handled. Due to 
it's dynamic nature adding members to this group is not allowed and adding it 
as a member to any other group would cause a cyclic membership.

Note however, that this validation is omitted in case of `Group.addMembers(String...)`
with `ImportBehavior.BESTEFFORT` (see above).

##### Cyclic Membership

Since Oak 1.7.0 the explicit check for cyclic group membership has been 
moved from the `Validator` to the `Group` implementation. As before cyclic 
membership might not be spotted and the membership resolution will log the 
cycle upon collection of all members/groups.

The following scenarios may leave the cycle unnoticed upon adding members:
- `Group.addMember(Authorizable)` when the editing `Session` cannot read all groups included in the cycle.
- `Group.addMembers(String...)` with `ImportBehavior.BESTEFFORT` where the member ID is not resolved.

See [OAK-3170] for additional information. 

### Configuration

Note that as of Oak 1.0 the implementation is responsible for defining the
content structure and will expand the multi-valued `rep:members` property accordingly.
Consequently, the following configuration option `groupMembershipSplitSize` present
with Jackrabbit 2.x is not supported anymore.

<!-- hidden references -->
[org.apache.jackrabbit.api.security.user.Group]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/security/user/Group.java
[org.apache.jackrabbit.api.security.user.Authorizable]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/security/user/Authorizable.java
[JCR-3880]: https://issues.apache.org/jira/browse/JCR-3880
[OAK-3170]: https://issues.apache.org/jira/browse/OAK-3170
