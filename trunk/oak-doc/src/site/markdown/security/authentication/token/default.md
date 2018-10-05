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

Token Management : The Default Implementation
---------------------------------------------

### General Notes

The default implementation of the token management API stores login tokens along
with the user's home directory in the repository. Along with the hash of the
login token separated properties defining the expiration time of the token
as well as as additional properties associated with the login tokens. This
additional information may be mandatory (thus validated during the login) or
optional. The optional properties are meant to have informative value only and
will be transferred to public attributes as exposed by the [AuthInfo] present
with each [ContentSession].

### Token Management Operations

#### Token Creation

The creation of a new token is triggered by valid and supported `Credentials` 
passed to the login module chain that contain an additional, empty `.token` attribute.

The [TokenLoginModule] will obtain these `Credentials` from the shared state
during the commit phase (i.e. phase 2 of the JAAS authentication) and will pass 
them to the configured [TokenProvider] implementation the following sequence:

    Credentials shared = getSharedCredentials();
    if (shared != null && tokenProvider.doCreateToken(shared)) {
        [...]
        TokenInfo ti = tokenProvider.createToken(shared);
        [...]
    }
    
In case of success these steps will have generated a new token and stored 
it's hash along with all mandatory and informative attributes to the new 
content node  representing the token.

##### Supported Credentials for Token Creation

By default the implementation deals with shared `SimpleCredentials`.

With Oak 1.5.8 the token management has been extended in order to allow 
for custom `Credentials` implementations. This is achieved by registering 
a custom implementation of the [CredentialsSupport] interface.
The default the token management uses [SimpleCredentialsSupport].

See also [OAK-4129] and section [Pluggability](#pluggability) below) for 
additional information.

#### Token Validation

Once a token has been created it can be used for subsequent repository 
logins with [TokenCredentials]. This time the [TokenLoginModule] will 
attempt to perform the login phase (i.e. phase 1 of the JAAS authentication).
 
This includes resolving the login token (`TokenProvider.getTokenInfo`) and
asserting it's validity in case it exists. The validation consists of following steps:

- check that the token has not expired (`TokenInfo.isExpired`)
- verify that all mandatory attributes are present and match the expectations (`TokenInfo.matches`)

Only if these steps have been successfully completed the login of the
[TokenLoginModule] will succeed.
 
#### Token Removal

A given login token (and the node associated with it) will be removed if 
the authentication fails due to an expired token or with an explicit API call 
i.e. `TokenInfo.remove`.

#### Resetting Expiration Time

The default `TokenProvider` implementation will automatically reset the expiration
time of a given token upon successful authentication.

This behavior can be disabled by setting the `tokenRefresh` configuration parameter
to `false` (see `PARAM_TOKEN_REFRESH` below). In this case expiration time will
not be reset and an attempt to do so using the API (e.g. calling `
TokenInfo.resetExpiration(long loginTime)`) will return `false` indicating
that the expiration time has not been reset. The token will consequently expire
and the user will need to login again using the configured login
mechanism (e.g. using the credentials support for token creation).

#### Token Cleanup

Automatic token cleanup can be enabled by setting the `tokenCleanupThreshold` parameter
to a value larger than `0` (`0` means disabled). This will trigger a cleanup call if
the number of tokens under a user exceeds this value. (As an implementation detail a
throttling method was introduced to only allow the call to go through 1/8 times).

This is available with Oak 1.7.12 on, see also [OAK-6818]for additional information.

<a name="representation"/>
### Representation in the Repository

#### Content Structure

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

#### Token Nodes

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

<a name="validation"/>
### Validation

The consistency of this content structure both on creation and modification is
asserted by a dedicated `TokenValidator`. The corresponding errors are
all of type `Constraint` with the following codes:

| Code              | Message                                                  |
|-------------------|----------------------------------------------------------|
| 0060              | Attempt to create reserved token property in other ctx   |
| 0061              | Attempt to change existing token key                     |
| 0062              | Change primary type of existing node to rep:Token        |
| 0063              | Creation/Manipulation of tokens without using provider   |
| 0064              | Create a token outside of configured scope               |
| 0065              | Invalid location of token node                           |
| 0066              | Invalid token key                                        |
| 0067              | Mandatory token expiration missing                       |
| 0068              | Invalid location of .tokens node                         |
| 0069              | Change type of .tokens parent node                       |

<a name="configuration"/>
### Configuration

The default Oak `TokenConfiguration` allows to define the following configuration
options for the `TokenProvider`:

#### Configuration Parameters

| Parameter                           | Type    | Default                  |
|-------------------------------------|---------|--------------------------|
| PARAM_TOKEN_EXPIRATION              | long    | 2 * 3600 * 1000 (2 hours)|
| PARAM_TOKEN_LENGTH                  | int     | 8                        |
| PARAM_TOKEN_REFRESH                 | boolean | true                     |
| PARAM_PASSWORD_HASH_ALGORITHM       | String  | SHA-256                  |
| PARAM_PASSWORD_HASH_ITERATIONS      | int     | 1000                     |
| PARAM_PASSWORD_SALT_SIZE            | int     | 8                        |
| PARAM_TOKEN_CLEANUP_THRESHOLD       | long    | 0 (no cleanup)           |
| | | |


<a name="pluggability"/>
### Pluggability

In an OSGi-based setup the default `TokenConfiguration` you can bind a 
custom implementation of the [CredentialsSupport] interface. Doing so 
allows to support any type of custom credentials, which do not reveal
the ID of the user logging into repository.

In particular when chaining the `TokenLoginModule` and the `ExternalLoginModule`
the [CredentialsSupport] can be used to authenticate and synchronize 
users provided by third party systems during phase 1 (login) and generate 
a login token during phase 2 (commit). See section 
[Authentication with the External Login Module](../externalloginmodule.html)
for additional details. For this to work the same [CredentialsSupport] 
must be configured with the [ExternalIdentityProvider] and the `TokenConfiguration`
and `CredentialsSupport.getUserId` must reveal the ID of the synced user (i.e. `ExternalUser.getId`).

In general the following steps are required in order to plug a different `CredentialsSupport`
into the default `TokenConfiguration`:

- implement the `CredentialsSupport` interface (e.g. as extension to the `ExternalIdentityProvider`)
- make sure the implementation is an OSGi service and deploy it to the Oak repository.

##### Examples

###### Example CredentialsSupport

In an OSGi-based setup it's sufficient to make the service available to the repository
in order to enable a custom `CredentialsSupport`.

    @Component
    @Service(value = {CredentialsSupport.class})
    /**
     * Custom implementation of the {@code CredentialsSupport} interface.
     */
    final class MyCredentialsSupport implements CredentialsSupport {

        @Nonnull
        @Override
        public Set<Class> getCredentialClasses() {
            return ImmutableSet.<Class>of(MyCredentials.class);
        }

        @CheckForNull
        @Override
        public String getUserId(@Nonnull Credentials credentials) {
            if (credentials instanceof MyCredentials) {
                // TODO: resolve user id
                return resolveUserId(credentials);
            } else {
                return null;
            }
        }

        @Nonnull
        @Override
        public Map<String, ?> getAttributes(@Nonnull Credentials credentials) {
            // TODO: optional implementation
            return ImmutableMap.of();
        }

        @Override
        public boolean setAttributes(@Nonnull Credentials credentials, @Nonnull Map<String, ?> attributes) {
           // TODO: optional implementation
           return false;
        }
        
        [...]
    }
    
###### Example CredentialsSupport in Combination with External Authentication

See section [Authentication with the External Login Module](../externalloginmodule.html#pluggability) 
for an example.

<!-- references -->

[TokenCredentials]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/security/authentication/token/TokenCredentials.java
[AuthInfo]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/AuthInfo.html
[ContentSession]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/ContentSession.html
[TokenProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/token/TokenProvider.html
[TokenInfo]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/token/TokenInfo.html
[TokenLoginModule]: /oak/docs/apidocs/org/apache/jackrabbit/oak/security/authentication/token/TokenLoginModule.html
[CredentialsSupport]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/credentials/CredentialsSupport.html
[SimpleCredentialsSupport]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/credentials/SimpleCredentialsSupport.html
[OAK-4129]: https://issues.apache.org/jira/browse/OAK-4129
[ExternalIdentityProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authentication/external/ExternalIdentityProvider.html