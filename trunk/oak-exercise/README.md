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

Jackrabbit Oak - Exercise
=======================================================

Oak module providing exercises for developers who wish to become familiar with
the Oak code base and understand the design principals and implementation
details.

NOTE: This module is not suited as reference for 'best-practises' for JCR
and Jackrabbit API consumers. Instead it often uses low-level implementation
access to illustrate a particular pattern or detail.

Mandatory Preparation
---------------------

Apart from the exercise code, you need have to following source packges
installed

Please make sure you have the following source code on your computer and setup in your preferred IDE

- Oak (http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/, https://github.com/apache/jackrabbit-oak)
- JCR 2.0 (https://svn.java.net/svn/jsr-283~svn/trunk/src/)
- Jackrabbit API (http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-api/,  https://github.com/apache/jackrabbit)
- Jackrabbit Commons (http://svn.apache.org/repos/asf/jackrabbit/trunk/jackrabbit-jcr-commons/, https://github.com/apache/jackrabbit)


Recommended Reading
-------------------

As preparation of the introduction it is recommended to take a look at security related sections of the JSR283 and to make yourself familiar with the security related areas of the API as well with the extensions defined in Apache Jackrabbit.
JSR 283 : Content Repository for Java Technology API Specification v2.0

The following sections of the specification deal with security in particular with authentication and authorization.

- 4.2 Login (http://www.day.com/specs/jcr/2.0/4_Connecting.html#4.2%20Login)
- 4.3 Impersonate (http://www.day.com/specs/jcr/2.0/4_Connecting.html#4.3%20Impersonate)
- 4.4.1 User (http://www.day.com/specs/jcr/2.0/4_Connecting.html#4.4.1%20User)
- 9 Permissions and Capabilities (http://www.day.com/specs/jcr/2.0/9_Permissions_and_Capabilities.html)
- 16 Access Control Management (http://www.day.com/specs/jcr/2.0/16_Access_Control_Management.html)

You can also find the correspoding parts of the API documentation:

- Login and Impersonation
        http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/Repository.html#login%28javax.jcr.Credentials,%20java.lang.String%29
        http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/Session.html#impersonate%28javax.jcr.Credentials%29
- Credentials
        http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/Credentials.html
        http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/GuestCredentials.html
        http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/SimpleCredentials.html
- Permission Discovery and JCR Action Constants
        http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/Session.html
        http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/Session.html#hasPermission%28java.lang.String,%20java.lang.String%29
        http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/Session.html#checkPermission%28java.lang.String,%20java.lang.String%29
- Access Control Management
        http://www.day.com/specs/javax.jcr/javadocs/jcr-2.0/javax/jcr/security/package-summary.html

#### Apache Jackrabbit : API Extensions for JSR 283

The JavaDoc for the latest Jackrabbit API (2.10) can be found at http://jackrabbit.apache.org/api/2.10/

- Authentication (TokenCredentials)
    http://jackrabbit.apache.org/api/2.10/org/apache/jackrabbit/api/security/authentication/token/TokenCredentials.html
- Access Control Management
    http://jackrabbit.apache.org/api/2.10/org/apache/jackrabbit/api/security/package-summary.html
- Privilege Management
    http://jackrabbit.apache.org/api/2.10/org/apache/jackrabbit/api/security/authorization/PrivilegeManager.html
- Principal Management
    http://jackrabbit.apache.org/api/2.10/org/apache/jackrabbit/api/security/principal/package-summary.html
- User Management
    http://jackrabbit.apache.org/api/2.10/org/apache/jackrabbit/api/security/user/package-summary.html

#### Oak Documentation

There exists some documentation about Oak Security at http://jackrabbit.apache.org/oak/docs/security/overview.html. That should cover everything that is being looked at during that introduction.
Further References
Java Authentication and Authorization Service (JAAS)

Note that we only make use of the Authentication part of JAAS. Nevertheless the following documents might be useful to consult when you are dealing with authentication.

- Reference Guide
    http://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/JAASRefGuide.html
- Develop a LoginModule
    http://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/JAASLMDevGuide.html

#### Java API References

The following API references to some authentication related classes that we keep referring to when talking about security in the repository
General Security

- Principal (http://docs.oracle.com/javase/7/docs/api/java/security/Principal.html)
- Group (http://docs.oracle.com/javase/7/docs/api/java/security/acl/Group.html)

#### Authentication

- LoginContext (http://docs.oracle.com/javase/7/docs/api/javax/security/auth/login/LoginContext.html )
- LoginModule (http://docs.oracle.com/javase/7/docs/api/javax/security/auth/spi/LoginModule.html)
- Configuration (http://docs.oracle.com/javase/7/docs/api/javax/security/auth/login/Configuration.html)
- Subject (http://docs.oracle.com/javase/7/docs/api/javax/security/auth/Subject.html)


Using the Exercise Module
-------------------------

TODO

How to Verify your Solutions
----------------------------

TODO