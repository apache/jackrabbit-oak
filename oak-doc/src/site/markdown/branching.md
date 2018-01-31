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

# Branching

Following a quick reminder on how to do branching for a stable release.

It's based on the 1.8 branching, fix the commands where needed. For 
details about each option please refer to the 
[official documentation](http://maven.apache.org/maven-release/maven-release-plugin/branch-mojo.html).

## 1. Test the command locally

    $ mvn release:branch -DbranchName=1.8 \
      -DbranchBase=https://svn.apache.org/repos/asf/jackrabbit/oak/branches \
      -DupdateBranchVersions=true -DreleaseVersion=1.8.0-SNAPSHOT \
      -DdryRun=true

You will be prompted for the next trunk version. In our example: `1.10-SNAPSHOT`.

Check that the following files contains the right versions

    pom.xml.branch        # should be 1.8.0-SNAPSHOT
    pom.xml.next          # should be 1.10-SNAPSHOT
    pom.xml.releaseBackup # should be 1.8-SNAPSHOT
    
## 2. Revert the local changes

You'll have a bunch of files locally that are not committed to svn.
Just to have a clean situation clean up everything

    $ svn st | grep '^?' | awk '{print $2}' | xargs rm
    
## 3. Actual branching

Re-execute the first command **without** the `-DdryRun=true`

    $ mvn release:branch -DbranchName=1.8 \
      -DbranchBase=https://svn.apache.org/repos/asf/jackrabbit/oak/branches \
      -DupdateBranchVersions=true -DreleaseVersion=1.8.0-SNAPSHOT \

Now you can checkout the branch and proceed with the release as normal.

## 4. Final Checks and leftovers

`release:branch` will leave behind the `oak-doc`'s pom. You'll have to 
manually update the version referring to the new `oak-parent`. 
See [r1820738](http://svn.apache.org/r1820738) for an example.

## References

- http://maven.apache.org/maven-release/maven-release-plugin/branch-mojo.html
- http://maven.apache.org/maven-release/maven-release-plugin/examples/branch.html
- http://jackrabbit.apache.org/jcr/creating-releases.html
