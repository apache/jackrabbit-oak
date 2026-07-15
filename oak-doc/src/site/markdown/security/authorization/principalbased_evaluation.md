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

Permission Evaluation with Principal-Based Authorization
--------------------------------------------------------------------------------

The examples below describe permission evaluation based on an authorization setup with the following characteristices:
   
    FilterProviderImpl:
        - Path: "/home/users/system/supported"

    CompositeAuthorizationConfiguration (CompositionType=AND): 
       
        PrincipalBasedAuthorizationConfiguration
        - FilterProvider: reference to FilterProviderImpl as configured above (default implementation)
        - Enable AggregationFilter: true
        - Ranking: 500
        
        AuthorizationConfigurationImpl
        - Ranking: 100
 
The following principals will be used in various combinations:
 
    Principals not supported by the configured 'FilterProvider'
    
    - GroupPrincipal:      'testgroup'        
    - Principal:           'user'        
    - SystemUserPrincipal: 'service-A' with path /home/users/system/45
    
    Principals supporeted by the configured 'FilterProvider'
    
    - SystemUserPrincipal: 'service-B' with path /home/users/system/supported/featureB/11
    - SystemUserPrincipal: 'service-C' with path /home/users/system/supported/featureC/C1
    
The following access control setup has been defined:
 
    grant access using regular path-based ac-management calls 
    i.e. 'AccessControlManager.getApplicablePolicies(String)' or 'AccessControlManager.getPolicies(String))'
    
    - 'testgroup': /content [jcr:read, jcr:readAccessControl]
    - 'service-A': /content [jcr:versionManagement]
    - 'service-B': /content [jcr:read, jcr:modifyProperties]
    
    grant access using principal-based ac-management calls (only possible for supported principals)
    i.e. 'JackrabbitAccessControlManager.getApplicablePolicies(Principal)' or 'AccessControlManager.getPolicies(Principal))'
    
    - 'service-B': /content [jcr:read, jcr:nodeTypeManagement]
    - 'service-C': /content [jcr:read, jcr:lockManagement]
                   
##### Example 1: Subject with principals _'user'_, _'testgroup'_
Since neither 'user' nor 'testgroup' is a system-user-princial supported by principal-based authorization, 
principal-based permission evaluation is omitted.

Result: the Session is granted `jcr:read`, `jcr:readAccessControl` at /content.
 
##### Example 2: Subject with principals _'service-A'_, _'testgroup'_
Since neither 'service-A' nor 'testgroup' is supported by principal-based authorization, 
principal-based permission evaluation is omitted.

Result: the Session is granted `jcr:read`, `jcr:readAccessControl`, `jcr:versionManagement` at /content.
 
##### Example 3: Subject with principals _'service-B'_, _'testgroup'_
Since 'testgroup' is not supported by principal-based authorization, principal-based permission evaluation is omitted 
and only path-based access control setup take effect.

Result: the Session is granted `jcr:read`,`jcr:readAccessControl`,`jcr:modifyProperties` at /content.
 
##### Example 4: Subject with principals _'service-A'_, _'service-B'_
Since 'service-A' is not supported by principal-based authorization, principal-based permission evaluation is omitted 
and only path-based access control setup take effect.

Result: the Session is granted `jcr:read`,`jcr:modifyProperties`,`jcr:versionManagement` at /content.
 
##### Example 5: Subject with principals _'service-B'_
'service-B' is supported by principal-based authorization and no unsupported principal is present in the Subject.
Therefore, principal-based permission evaluation takes effect. Since the `AggregationFilter` is enabled in the configuration 
describedf above, permission evaluation stops and does not continue evaluating path-based permissions.

Result: the Session is granted `jcr:read`, `jcr:nodeTypeManagement` at /content.

NOTE: 
If `AggregationFilter` was disabled _both_ permission providers would be used for the evaluation. 
The result then depends on the `CompositionType`:

| `AggregationFilter` | `CompositionType.AND` | `CompositionType.OR` |
|---------------------|-----------------------|----------------------|
| enabled           | `jcr:read`, `jcr:nodeTypeManagement` | `jcr:read`, `jcr:nodeTypeManagement` |
| disabled          | `jcr:read`          | `jcr:read`, `jcr:modifyProperties`, `jcr:nodeTypeManagement` |
 
##### Example 6: Subject with principals _'service-C'_
'service-C' is supported by principal-based authorization and no unsupported principal is present in the Subject.
Therefore, principal-based permission evaluation takes effect. Since the `AggregationFilter` is enabled in the configuration
described above, permission evaluation stops and does not continue evaluating path-based permissions.

Result: the Session is granted `jcr:read`, `jcr:lockManagement` at /content.

NOTE: 
If `AggregationFilter` was disabled _both_ permission providers would be used for the evaluation. 
The result then depends on the `CompositionType`:

| `AggregationFilter` | `CompositionType.AND` | `CompositionType.OR` |
|---------------------|-----------------------|----------------------|
| enabled           | `jcr:read`, `jcr:lockManagement` | `jcr:read`, `jcr:lockManagement` |
| disabled          | -                   | `jcr:read`, `jcr:lockManagement` |
 
##### Example 6: Subject with principals _'service-B'_, _'service-C'_
Both 'service-B' and 'service-C' are supported by principal-based authorization and no unsupported principal is present in the Subject.
Therefore, principal-based permission evaluation takes effect. Since the `AggregationFilter` is enabled in the configuration
described above, permission evaluation stops and does not continue evaluating path-based permissions.

Result: the Session is granted `jcr:read`, `jcr:nodeTypeManagement`, `jcr:lockManagement` at /content.

NOTE: 
If `AggregationFilter` was disabled _both_ permission providers would be used for the evaluation. 
The result then depends on the `CompositionType`:

| `AggregationFilter` | `CompositionType.AND` | `CompositionType.OR` |
|---------------------|-----------------------|----------------------|
| enabled           | `jcr:read`, `jcr:nodeTypeManagement`, `jcr:lockManagement` | `jcr:read`, `jcr:nodeTypeManagement`, `jcr:lockManagement` |
| disabled          | `jcr:read`          | `jcr:read`, `jcr:modifyProperties`, `jcr:nodeTypeManagement`, `jcr:lockManagement` |
          
    
 
