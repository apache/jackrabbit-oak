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

Token Authentication and Token Management
--------------------------------------------------------------------------------

### General

The token based authentication has been completely refactor in Oak and has the
following general characteristics.

- Dedicated API for managing login tokens defined in the package `org.apache.jackrabbit.oak.spi.security.authentication.token`.
- Pluggable configuration of the new token management API
- Complete separation of token based authentication into a separate `LoginModule`.

### Token Authentication

As of Oak the token based authentication is handled by a dedicated [TokenLoginModule].
It is both responsible for issueing new login tokens and validating [TokenCredentials]
passed to the repository login.

This token specific login module implementation obtains the [TokenProvider] from
the security configuration as defined for the content repository. The token
management implementation present with a given repository can be changed or
extended at runtime (see section Configuration below).

#### TokenLoginModule

The `TokenLoginModule` itself behaves as follows:

*Phase 1: Login*
_todo_

*Phase 1: Commit*
_todo_

### Token Management API

_todo_

- [TokenProvider]
- [TokenInfo]
- [CompositeTokenProvider]


### Default Implementation

The default implementation of the token management API stores login tokens along
with the user's home directory in the repository. Along with the hash of the
login token separated properties defining the expiration time of the token
as well as as additional properties associated with the login tokens. This
additional information may be mandatory (thus validated during the login) or
optional. The optional properties are meant to have informative value only and
will be transferred to public attributes as exposed by the [AuthInfo] present
with each [ContentSession].

#### Token Creation

_todo_

#### Token Removal

_todo_

#### Resetting Expiration Time

_todo_


#### Token Representation in the Repository

##### Content Structure

The login tokens issued for a given user are all located underneath a node
named `.tokens` that will be created by the `TokenProvider` once the first token
is created. The default implementation creates a distinct node for each login
token as described below

    testUser {
        "jcr:primaryType": "rep:User",
        ...
        ".tokens" {
            "jcr:primaryType": "rep:Unstructured",
            "2014-04-10T16.09.07.159+02.00" {
                "jcr:primaryType": "rep:Token",
                ...
            "2014-05-07T12.08.57.683+02.00" {
                "jcr:primaryType": "rep:Token",
                ...
            }
            "2014-06-25T16.00.13.018+02.00" {
                "jcr:primaryType": "rep:Token",
                ...
            }
        }
    }

##### Token Nodes

As of Oak 1.0 the login token are represented in the repository as follows:

- the token node is referenceable with the dedicated node type `rep:Token` (used to be unstructured in Jackrabbit 2.x)
- expiration and key properties are defined to be mandatory and protected
- expiration time is obtained from `PARAM_TOKEN_EXPIRATION` specified in the
  login attributes and falls back to the configuration parameter with the same
  name as specified in the configuration options of the `TokenConfiguration`.

The definition of the new built-in node type `rep:Token`:

    [rep:Token] > mix:referenceable
    - rep:token.key (STRING) protected mandatory
    - rep:token.exp (DATE) protected mandatory
    - * (UNDEFINED) protected
    - * (UNDEFINED) multiple protected

The following example illustrates the token nodes resulting from this node type
definition:

    testUser {
            "jcr:primaryType": "rep:User",
            ...
            ".tokens" {
                "2014-04-10T16.09.07.159+02.00" {
                    "jcr:primaryType": "rep:Token",
                    "jcr:uuid": "30c1f361-35a2-421a-9ebc-c781eb8a08f0",
                    "rep:token.key": "{SHA-256}afaf64dba5d862f9-1000-3e2d4e58ac16189b9f2ac95d8d5b692e61cb06db437bcd9be5c10bdf3792356a",
                    "rep:token.exp": "2014-04-11T04:09:07.159+02:00",
                    ".token.ip": "0:0:0:0:0:0:0:1%0"
                    ".token.otherMandatoryProperty": "expectedValue",
                    "referer": "http://localhost:4502/crx/explorer/login.jsp"
                    "otherInformalProperty": "somevalue"
                },
                "2014-05-07T12.08.57.683+02.00" {
                    "jcr:primaryType": "rep:Token",
                    "jcr:uuid": "c95c91e2-2e08-48ab-93db-6e7c8cdd6469",
                    "rep:token.key": "{SHA-256}b1d268c55abda258-1000-62e4c368972260576d37e6ba14a10f9f02897e42992624890e22c522220f7e54",
                    "rep:token.exp": "2014-05-08T00:08:57.683+02:00"
                },
                ...
            }
        }
    }

### Configuration

_todo_

#### Custom TokenProvider

_todo_


<!-- references -->

[TokenLoginModule]: /oak/docs/apidocs/org/apache/jackrabbit/oak/security/authentication/token/TokenLoginModule.html
[TokenCredentials]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/security/authentication/token/TokenCredentials.java
[AuthInfo]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/AuthInfo.html
[ContentSession]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/ContentSession.html
[TokenProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/token/TokenProvider.html
[TokenInfo]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/token/TokenInfo.html
[CompositeTokenProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/token/CompositeTokenProvider.html