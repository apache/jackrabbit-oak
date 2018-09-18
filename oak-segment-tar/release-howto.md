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

*NOTE*: The information in this file is outdated since [OAK-5007](https://issues.apache.org/jira/browse/OAK-5007) aligned this module's release cycle with the rest of Oak.

*TODO*: Remove or update this file once we clarified the release modalities for Segment Tar wrt. Oak 1.8   

# Release HOWTO

This HOWTO explains how to release Oak Segment Tar. I describe the steps required to release version `$VERSION`, assuming that the latest released version is `$PREV` and the next is `$NEXT`.

## Vote

1. Execute `mvn release:prepare`.
2. Execute `mvn release:perform`.
3. Close the staged Maven repository.
    1. Log in to https://repository.apache.org.
    2. Click on "Staging Repositories".
    3. Click on the repository.
    4. Click on "Close".
4. Start the vote on oak-dev. See the vote template below.
5. Wait 72 hours.

A vote is successful if it has at least three +1 and if the number of -1 is less than the number of +1.

## Successful vote

1. Close the vote on oak-dev. See the results template below.
2. Copy the artifacts to the release repository.
    1. Execute the `check_staged_release.sh` script if you haven't already.
    2. Move to the temporary folder created by the script where the artifacts are.
    3. Execute `svn import https://dist.apache.org/repos/dist/release/jackrabbit/oak/oak-segment-tar/$VERSION`.
3. Delete the old release.
    1. Execute `svn rm https://dist.apache.org/repos/dist/release/jackrabbit/oak/oak-segment-tar/$PREV`.
4. Release the staged Maven repository.
    1. Log in to https://repository.apache.org.
    2. Click on "Staging Repositories".
    3. Click on the repository.
    4. Click on "Release".
5. Release the version on Jira.
    1. Go to https://issues.apache.org/jira/plugins/servlet/project-config/OAK/versions.
    2. Create version $NEXT if it doesn't already exist.
    3. Move your mouse on the $VERSION version.
    4. Click on the gear icon.
    5. Click on release.
    6. Select "Move issues to version:".
    7. Select version $NEXT from the dropdown box.
    8. Enter the release date. The release date is the day the vote ended.

## Unsuccessful vote

1. Remove the release tag from Subversion.
    1. Execute `svn rm http://svn.apache.org/repos/asf/jackrabbit/oak/tags/oak-segment-tar-$VERSION`.
2. Drop the staged Maven repository.
    1. Log in to https://repository.apache.org.
    2. Click on "Staging Repositories".
    3. Click on the repository.
    4. Click on "Drop".
3. Rollback the version information in `pom.xml`.

## Vote template

Substitute the following information in the body of the mail:

- The number of fixed and closed issues.
- The URL of the version in Jira.
- The number of the staging repository.

This information is not a function of the released version, so it has to be manually entered at every release.

```
Subject: [VOTE] Release Apache Jackrabbit Oak Segment Tar version $VERSION

Hi,

We solved 5 issues in this release:
https://issues.apache.org/jira/browse/OAK/fixforversion/12337966

There are still some outstanding issues:
https://issues.apache.org/jira/browse/OAK/component/12329487

Staging repository:
https://repository.apache.org/content/repositories/orgapachejackrabbit-1162

You can use this UNIX script to download the release and verify the signatures:
http://svn.apache.org/repos/asf/jackrabbit/oak/trunk/check_staged_release.sh

Usage:
sh check_staged_release.sh 1162 /tmp/oak-staging

Please vote to approve this release:

  [ ] +1 Approve the release
  [ ]  0 Don't care
  [ ] -1 Don't release, because ...

This majority vote is open for at least 72 hours.
```

## Result template

```
[RESULT][VOTE] Release Apache Jackrabbit Oak Segment Tar version $VERSION

Hi,

The vote passes as follows:

+1 First voter
+1 Second voter
+1 Third voter

Thanks for voting. I'll push the release out.
```
