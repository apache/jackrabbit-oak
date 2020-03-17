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

# Developing with Git

Oak codebase is backed by SVN, not git. The github location is a
mirror that could lag hours behind.

Following an example on how to start on a feature named:
FEATURE. Where you'll work and commit against your fork on github and
eventually produce a patch to be attached to the jira issue or commit
it straight to svn (assuming you have rights).

## Initialising the local environment

First of all you need to [fork the github repo](http://github.com/apache/jackrabbit-oak).

    # clone your fork (as origin) into ./jackrabbit-oak
    git clone https://github.com/YOUR_USER/jackrabbit-oak.git

    cd jackrabbit-oak

    # Adding the oak GH repo as upstream
    git remote add upstream https://github.com/apache/jackrabbit-oak.git

    # take the upstream trunk
    git fetch upstream
    git checkout -b upstream-trunk upstream/trunk

    # take the latest Apache's authors file for git svn
    curl https://git-wip-us.apache.org/authors.txt -o .git/authors.txt
    git config svn.authorsfile .git/authors.txt

    # init git svn 
    git svn init --prefix=upstream/ --tags=tags --trunk=trunk \
        --branches=branches \
        https://svn.apache.org/repos/asf/jackrabbit/oak
        
## Creating a new feature branch

Now it's possible to start working on a feature branch like this

    # Ensure you're in upstream-trunk
    git checkout upstream-trunk

    # align with svn
    git svn rebase

    # create a new feature branch
    git checkout -b FEATURE

    # push it to your fork
    git push --set-upstream origin FEATURE

## Work on the feature branch

Now the work can be carried out easily on the feature branch by
keeping it regularly aligned with svn and pushing it to the fork

    # Merge the latest trunk from svn
    git checkout upstream-trunk
    git svn rebase
    git checkout FEATURE
    git merge upstream-trunk

    # your normal work and commits in FEATURE

    # sending the commits to github
    git push
    
## Producing a patch against trunk

Now the work is completed and you want to attach a patch to the jira
issue for commit and/or review

    git checkout upstream-trunk
    git svn rebase
    git checkout FEATURE
    git merge upstream-trunk

    # solve any conflicts

    git push

    # producing the patch file
    git diff -w -p upstream-trunk > ~/FEATURE.patch


## Committing to svn

As for the patch, the work is complete and you want to commmit to svn
(assuming you have rights)

    # getting the latest svn
    git checkout upstream-trunk
    git svn rebase
    git checkout FEATURE
    git merge upstream-trunk
    git push

    # taking all the branch commit into upstream as one big commit
    git checkout upstream-trunk
    git merge --squash --no-commit FEATURE

    # committing all the changes as git
    git commit -a

    # committing all the changes to svn
    git svn dcommit --username=goofy --no-rebase

    # --username is needed only the first commit or if differs from
    #   the currently logged in
    # --no-rebase is useful if you're EU based as the apache svn EU
    #   mirror lag behind some seconds.
    #
    # if in EU wait 10 seconds and give a `git svn rebase` to properly
    #   align your upstream later on.
    
## References

- https://wiki.apache.org/general/GitAtApache
