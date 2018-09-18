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

# Cutting diagnostic builds

The cutting of a diagnostic build, is the process where you want to
deliver one or more oak bundles, let's say `oak-core` into a specific
environment in order to assess whether it actually solves the issues.

What you are aiming is to eventually produce a bundle in the format
of, for example, `oak-core-1.0.22-R2707077`.

Let's see it through an example.

We want to produce a diagnostic build of `oak-core` for what it will
be Oak **1.0.23**. It means we currently have in our `pom.xml` a
version of `<version>1.0.23-SNAPSHOT</version>`.

## What version shall I use?

Open the svn directory where the 1.0 branch is and issue a

    $ svn up
    $ svn info

you will see something like

    Working Copy Root Path: /apache/oak-svn-1.0
    URL: https://svn.apache.org/repos/asf/jackrabbit/oak/branches/1.0
    Repository Root: https://svn.apache.org/repos/asf
    Repository UUID: 13f79535-47bb-0310-9956-ffa450edef68
    Revision: 1708581
    Node Kind: directory
    Schedule: normal
    Last Changed Author: chetanm
    Last Changed Rev: 1708547
    Last Changed Date: 2015-10-14 06:56:40 +0100 (Wed, 14 Oct 2015)

what you're interested is the revision number. In our case: `1708581`.

This means you'll produce a bundle with a version of
`1.0.22-R2708581`.

**Note that the produced version is lower then the official release
  you're working on. 1.0.22 vs 1.0.23**
  
**Note to use the '-R' (uppercase) instead of '-r' (lowercase) as it
  will be lower than '-SNAPSHOT'. Doing otherwise will result in
  troubles when trying to apply a '-SNAPSHOT' version on top of the
  internal build**

If you're in doubt about what versioning and how OSGi or Maven will
behave have a look at the
[Versionatorr App](http://versionatorr.appspot.com/). You want your
diagnostic build to be **always less than** the oak version where your
fix is going to be released.

## Changing the version in all the poms.

Now that you know you want to produce `1.0.22-R2708581` you have to
change all the poms accordingly.

Go into `oak-parent` and issue the following maven command.

    oak-parent$ mvn versions:set -DnewVersion=1.0.22-R2708581

you may encounter the following exception. Simply ignore it. Nothing
went wrong.

    java.io.FileNotFoundException: /oak-svn-1.0/oak-parent/oak-parent (No such file or directory)

## Building the release

Now you can build the release as usual

    jackrabbit-oak$ mvn clean install

and you'll have a full oak build with the version
`1.0.22-R2708581`. Go into `oak-core/target` and take the produced
jar.

## Re-setting the svn branch

You don't want to commit the changes back to svn so we reset the
branch as the original state

    jackrabbit-oak$ mvn versions:revert

