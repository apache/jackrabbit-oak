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

Searching Users and Groups
--------------------------------------------------------------------------------

The user management API provided with Jackrabbit comes with a dedicated query
API that allows for searching authorizables indepedant of the underlying user
management implementation.

### Lookup a Single Authorizable

- `UserManager.getAuthorizable(String)` : lookup by id
- `UserManager.getAuthorizable(Principal` : lookup by principal
- `UserManager.getAuthorizableByPath(String)` : lookup by path

###### Examples

    Authorizable a = userMgr.getAuthorizable("jackrabbit");
    Authorizable a2 = userManager.getAuthorizableByPath(a.getPath());
    Authorizable everyoneGroup = userManager.getAuthorizable(EveryonePrincipal.getInstance());


### Searching for Authorizables

- `UserManager.findAuthorizables(String relPath, String value)`
- `UserManager.findAuthorizables(String relPath, String value, int searchType)`
- `UserManager.findAuthorizables(Query query)`

### The Query API

- [Query]: The query object passed to the findAuthorizable method. It has a single `build(QueryBuilder)` method.
- [QueryBuilder]: The query builder as described below.

#### QueryBuilder

##### Find by Property

The following conditions can be used to find authorizables by properties:

- `QueryBuilder.eq(String relPath, Value)`: holds if property at relPath is _equal_ to the specified value.
- `QueryBuilder.neq(String relPath, Value)`: holds if property at relPath is _not equal_ to the specified value.
- `QueryBuilder.lt(String relPath, Value)`: holds if property at relPath is _smaller_ than the specified value.
- `QueryBuilder.le(String relPath, Value)`: holds if property at relPath is _smaller or equal_ than/to the specified value.- QueryBuilder.lt(String relPath, Value) : matches if property value at relPath is _smaller_ than the specified value.
- `QueryBuilder.gt(String relPath, Value)`: holds if property at relPath is _greater_ than the specified value.
- `QueryBuilder.ge(String relPath, Value)`: holds if property at relPath is _greater or equal_ than/to the specified value.
- `QueryBuilder.ge(String relPath, Value)`: holds if property at relPath is _greater or equal_ than/to the specified value.
- `QueryBuilder.contains(String relPath, String searchExpr)`: full text search.
- `QueryBuilder.exists(String relPath)`: holds if a property at relPath exists.

###### Examples

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.eq("@name", vf.createValue("jackrabbit")));
        }
    });

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.gt("profile/@weight", vf.createValue(200.0)));
        }
    });

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.contains("profile/@color", "gold"));
        }
    });

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.exists("@poisonous"));
        }
    });

##### Find by Pattern

The following conditions allow to specify a search pattern, where '%' represents
any string of zero or more characters and '_' represents any single character.

- `QueryBuilder.like(String relPath, String pattern)`: holds if a property relPath matches the pattern.
- `QueryBuilder.like(String relPath, String pattern)`: holds if a property relPath matches the pattern.
- `QueryBuilder.nameMatches(String pattern)`: filter by principal name (see below)

###### Examples

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
      public <T> void build(QueryBuilder<T> builder) {
          builder.setCondition(builder.like("profile/@food", "c%"));
      }
    });

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
      public <T> void build(QueryBuilder<T> builder) {
          builder.setCondition(builder.like("profile/@food", "c_t"));
      }
    });

##### Find in Any Property

Use "." to indicate that properties with any name at a given relative path should
be included in the search result.

###### Examples

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.contains(".", ""jackrabbit""));
        }
    });

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.contains("profile/.", "gold"));
        }
    });

##### Multiple Conditions

- `QueryBuilder.and(Condition condition1, Condition condition2)`: holds if both sub conditions hold
- `QueryBuilder.or(Condition condition1, Condition condition2)`: holds if any of the two sub conditions hold
- `QueryBuilder.not(Condition condition)`: holds if the sub condition does not hold.

###### Examples

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.and(
                builder.eq("profile/@cute", vf.createValue(true)),
                builder.eq("profile/@color", vf.createValue("black"))
            ));
        }
    });

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.or(
                builder.eq("profile/@food", vf.createValue("mice")),
                builder.eq("profile/@food", vf.createValue("nectar"))
                )
            );
        }
    });

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.not(builder.exists("profile/@food"))
            ));
        }
    });

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.and(
                builder.eq("profile/@cute", vf.createValue(true)),
                builder.not(builder.eq("profile/@color", vf.createValue("black")))
            ));
        }
    });

##### Sort Results

- `QueryBuilder.setSortOrder(String propertyName, Direction direction)`
- `QueryBuilder.setSortOrder(String propertyName, Direction direction, boolean ignoreCase)`

where direction can be either of

- `Direction.ASCENDING`
- `Direction.DESCENDING`

###### Examples

    final boolean ignoreCase = true;
    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.exists("profile/@weight"));
            builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING, ignoreCase);
        }
    });

##### Set Limits

- `QueryBuilder.setLimit(long offset, long maxCount)`
- `QueryBuilder.setLimit(Value bound, long maxCount)` : bound refers to the value
of the `setSortOrder(String, Direction)` property. The result is limited to
authorizables whose values of the sort order property follow `bound` in the sort direction.

###### Examples

    final long offset = 25;
    final long maxCount = 1000; // -1 for no limit
    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setLimit(offset, maxCount);
        }
    });

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.eq("profile/@cute", vf.createValue(true)));
            builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING, true);
            builder.setLimit(vf.createValue(1000.0), count);
        }
    });

##### Filter by Authorizable Type

- `QueryBuilder.setSelector(Class<? extends Authorizable> selector)`: Limit search
result to a specific type of authorizables (in the example: groups only)

The selector may take any of the following values:

- `Authorizable.class`
- `Group.class`
- `User.class`

###### Example

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setSelector(Group.class);
        }
    });

##### Find by Principal Name

- `QueryBuilder.nameMatches(String pattern)`: the pattern may include '_' and '%' (see above).

NOTE: the 'nameMatches' condition is a shortcut for a regular search for the
principal name, which in the default implementation is stored in `rep:principalName`.
It does not take any custom name properties into account nor query `rep:authorizableId`.

###### Examples

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.nameMatches("j%P"));
        }
    });

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.nameMatches("c_tP"));
        }
    });

##### Find Group Members

- `QueryBuilder.setScope(String groupName, boolean declaredOnly)`: will only return members of the group with the specified name.

###### Example

    final declaredMembersOnly = false;
    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setScope("mammals", declaredMembersOnly);
        }
    });

##### Search for Impersonators

- 'QueryBuilder.impersonates(String principalName)'

NOTE: this condition looks for authorizables that granted impersonation
to the authorizable with the specified principal name.

###### Example

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) {
            builder.setCondition(builder.impersonates("jackrabbitP"));
        }
    });

##### Find All

Find all authorizables accessible to the editing session

###### Example

    Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
        public <T> void build(QueryBuilder<T> builder) { /* any */ }
    });

### Characteristics of the Default Implementation

See sections [Differences to Jackrabbit 2.x](differences.html#query) and 
[The Default Implementation](default.html#query) for details.

### Utilities

See [org.apache.jackrabbit.commons.jackrabbit.user.AuthorizableQueryManager] for
a utility class provided by the jcr-commons module present with Jackrabbit.

<!-- hidden references -->

[QueryBuilder]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/security/user/QueryBuilder.java
[Query]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/security/user/Query.java
[org.apache.jackrabbit.commons.jackrabbit.user.AuthorizableQueryManager]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-jcr-commons/src/main/java/org/apache/jackrabbit/commons/jackrabbit/user/AuthorizableQueryManager.java