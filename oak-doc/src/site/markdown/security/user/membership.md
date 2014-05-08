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

##### Example Group with few members
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

##### Example Group with many members
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

*Note*: The exact threshold value that determines the storage strategy is an implementation detail and might even vary depending on the underlying persistence layer.

##### Upgrading Groups from Jackrabbit 2.x to OAK content structure

Upon upgrade from a Jackrabbit 2.x repository to OAK the group member lists that
adjusted to reflect the new content structure as created by the OAK user management
implementation; this means that the group member node structure will be changed
by the migration process. Applications that rely on these implementation
details of Jackrabbit 2.x user management instead of use the corresponding
API calls will need to be modified accordingly.

##### Importing Group Members
Importing group members through the import methods in `javax.jcr.Session` or
`javax.jcr.Workspace` is storage agnostic and supports both, property based and
node based, strategies and is backward compatible to content exported from
Jackrabbit 2.x. The group member lists that are modified during an import are
internally processed using the normal user manager APIs. This implies that the
node structure after the import might not be the same as the one represented in the input.