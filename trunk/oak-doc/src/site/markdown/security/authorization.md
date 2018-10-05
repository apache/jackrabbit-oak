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

Authorization
--------------------------------------------------------------------------------

### General Notes

One of main goals for Oak security, was to clearly separates between access control 
management (such as defined by the JCR and Jackrabbit API) and the internal
permission evaluation.
 
While access control management is defined to be an optional feature added in JCR 2.0,
permission evaluation was mandated since the very first version of JCR even though
it remained an implementation detail.

The documentation follows this separations and handles access control and permission
evaluation separately:

- [Access Control Management](accesscontrol.html)
- [Permissions](permission.html)

Despite the fact that there is a distinction between the public facing access
control management and the internal permission evaluation, these two topics remain
connected to one another and a given authorization model is expected to define and
handle both in a consistent manner. Consequently the main entry point for
authorization related operations is a single `AuthorizationConfiguration` (see 
section [configuration](#configuration) below).

<a name="api_extensions"/>
### API Extensions

The API extensions provided by Oak are covered in the following sections:

- [Access Control Management](accesscontrol.html#api_extensions)
- [Permissions](permission.html#api_extensions)
- [Restriction Management](authorization/restriction.html#api_extensions)

<a name="configuration"/>
### Configuration

The configuration of the authorization related parts is handled by the [AuthorizationConfiguration]. 
This class provides the following methods:

- `getAccessControlManager`: get a new ac manager instance (see [Access Control Management](accesscontrol.html)).
- `getPermissionProvider`: get a new permission provider instance (see [Permissions](permission.html)).
- `getRestrictionProvider`: get a new instance of the restriction provider (see [Restriction Management](authorization/restriction.html).

#### Configuration Parameters

The supported configuration options of the default implementation are described 
separately for [access control management](accesscontrol/default.html#configuration) 
and [permission evalution](permission/default.html#configuration) .

<a name="pluggability"/>
### Pluggability

There are multiple options for plugging authorization related custom implementations:

#### Aggregation of Different Authorization Models

##### Since Oak 1.4

As of Oak 1.4 the built-in `SecurityProvider` implementations allow for the 
aggregation of multiple `AuthorizationConfiguration`s.

The behaviour of the `CompositeAuthorizationConfiguration` is described in
the corresponding [section](authorization/composite.html) (see also [OAK-1268]).

##### Previous Versions

In previous versions of Oak aggregation of multiple authorization models was
not supported and it was only possible to replace the existing `AuthorizationConfiguration`.
This would completely replace the default way of handling authorization in the repository.

In OSGi-base setup this is achieved by making the configuration implementation a service
such that it takes precendece over the default. 

In a non-OSGi-base setup the custom configuration must be exposed by the `SecurityProvider` implementation.

#### Extending the Restriction Provider

In all versions of Oak it is possible to plug custom implementation(s) for the
restriction management that allows to narrow the effect of permissions to
items matching a given, defined behavior. Details can be found in section 
[RestrictionManagement](authorization/restriction.html#pluggability).

<a name="further_reading"/>
### Further Reading

- [Access Control Management](accesscontrol.html)
- [Permission Evalution](permission.html)
- [Restriction Management](authorization/restriction.html)
- [Combining Multiple Authorization Models](authorization/composite.html)

<!-- hidden references -->
[AuthorizationConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/authorization/AuthorizationConfiguration.html
[OAK-1268]: https://issues.apache.org/jira/browse/OAK-1268