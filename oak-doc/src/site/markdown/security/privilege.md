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

Privilege Management
--------------------------------------------------------------------------------

<a name="jcr_api"/>
### JCR API

As of JSR 283 the API contains the following privilege related interfaces and methods:

- `Privilege`: exposes the name and characteristics of a given privilege and provides constants for privilege names defined by JCR.
- `AccessControlManager.getSupportedPrivileges(String)` (see also `PrivilegeManager.getRegisteredPrivileges()`)
- `AccessControlManager.privilegeFromName(String)` equivalent to `PrivilegeManager.getPrivilege(String)`

<a name="jackrabbit_api"/>
### Jackrabbit API

Privilege management is outside of the scope provided by JCR and therefore provided
by the extensions defined by the Jackrabbit API. It consists of a single interface:

- [PrivilegeManager]: privilege discovery and registration of new custom privileges.
    - `getRegisteredPrivileges()`
    - `getPrivilege(String)`
    - `registerPrivilege(String, boolean, String[])

##### Examples

###### Access PrivilegeManager in JCR

    PrivilegeManager privilegeManager = session.getWorkspace().getPrivilegeManager();

###### Access PrivilegeManager in Oak

    Root root = contentSession.getLatestRoot();
    PrivilegeConfiguration config = securityProvider.getConfiguration(PrivilegeConfiguration.class);
    PrivilegeManager privilegeManage = config.getPrivilegeManager(root, namePathMapper));

###### Register Custom Privilege

    PrivilegeManager privilegeManager = session.getWorkspace().getPrivilegeManager();
    String privilegeName = ...
    boolean isAbstract = ...
    String[] declaredAggregateNames = ...
    // NOTE: workspace operation that doesn't require Session#save()
    privilegeManager.registerPrivilege(privilegeName, isAbstract, declaredAggregateNames);

<a name="api_extensions"/>
### API Extensions

- [PrivilegeConfiguration] : Oak level entry point to retrieve `PrivilegeManager` and privilege related configuration options.
- [PrivilegeConstants] : Constants related to privilege management such as Oak names of the built-in privileges.
- [PrivilegeBitsProvider] : Internal provider to read `PrivilegeBits` from the repository content and map names to internal representation (and vice versa).
- [PrivilegeBits]: Internal representation of JCR privileges.

<a name="utilities"/>
### Utilities

The jcr-commons module present with Jackrabbit provide some privilege related
utility methods:

- `AccessControlUtils`
    - `privilegesFromNames(Session session, String... privilegeNames)`
    - `privilegesFromNames(AccessControlManager accessControlManager, String... privilegeNames)`

<a name="default_implementation"/>
### Oak Privilege Management Implementation

The behavior of the default privilege management implementation is described in section 
[Privilege Management: The Default Implementation](privilege/default.html).

<a name="configuration"/>
### Configuration

The [PrivilegeConfiguration] is the Oak level entry point to obtain a new
`PrivilegeManager` as well as privilege related configuration options. The default
implementation of the `PrivilegeManager` interface is based on Oak API and can
equally be used for privilege related tasks in the Oak layer.

<a name="pluggability"/>
### Pluggability

_Please note:_ While it's in theory possible to replace the default privilege
management implementation in Oak, this is only recommended if you have in depth
knowledge and understanding of Jackrabbit/Oak internals and are familiar with
the security risk associated with it. Doing so, will most likely require a re-write
of the default access control and permission evaluation.

<a name="further_reading"/>
### Further Reading

- [Differences wrt Jackrabbit 2.x](privilege/differences.html)
- [Privilege Management : The Default Implementation](privilege/default.html)
- Mapping Privileges to Items and API Calls
    - [Mapping Privileges to Items](privilege/mappingtoitems.html)
    - [Mapping API Calls to Privileges](privilege/mappingtoprivileges.html)


<!-- references -->
[PrivilegeConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/privilege/PrivilegeConfiguration.html
[PrivilegeConstants]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/privilege/PrivilegeConstants.html
[PrivilegeBitsProvider]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/privilege/PrivilegeBitsProvider.html
[PrivilegeBits]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/security/privilege/PrivilegeBits.html
[PrivilegeManager]: http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/src/main/java/org/apache/jackrabbit/api/security/authorization/PrivilegeManager.java