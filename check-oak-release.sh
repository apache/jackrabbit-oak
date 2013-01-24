#!/bin/sh

## 
##    Licensed to the Apache Software Foundation (ASF) under one or more
##    contributor license agreements.  See the NOTICE file distributed with
##    this work for additional information regarding copyright ownership.
##    The ASF licenses this file to You under the Apache License, Version 2.0
##    (the "License"); you may not use this file except in compliance with
##    the License.  You may obtain a copy of the License at
## 
##      http://www.apache.org/licenses/LICENSE-2.0
## 
##    Unless required by applicable law or agreed to in writing, software
##    distributed under the License is distributed on an "AS IS" BASIS,
##    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##    See the License for the specific language governing permissions and
##    limitations under the License.
## 

VERSION="$1"
SHA="$2"

if [ -z "$VERSION" -o -z "$SHA" ]
then
 echo "Usage: $0 <version-number> <checksum> [temp-directory]"
 exit
fi

STAGING="https://dist.apache.org/repos/dist/dev/jackrabbit/oak/$VERSION/"

WORKDIR=${4:-target/oak-staging-`date +%s`}
mkdir -p "$WORKDIR"

echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] DOWNLOAD RELEASE CANDIDATE                                              "
echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] "
echo "[INFO] Downloading release candidate, please wait..."

if svn --quiet export "$STAGING" "$WORKDIR/$VERSION"; then
  echo "[INFO] Release downloaded."
else
  echo "[ERROR] Unable to download release from $STAGING"
  exit 1
fi

echo "[INFO] "
echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] CHECK SIGNATURES AND DIGESTS                                            "
echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] "

## 1. check sha from release email against src.zip.sha file

downloaded_sha=$(cat `find "$WORKDIR" -type f | grep "jackrabbit-oak-$VERSION-src.zip.sha"`)
echo "[INFO] Step 1. Check release cheksum"
if [ $SHA = $downloaded_sha ]; then
  echo "[INFO] Release checksum matches provided checksum."
else
  echo "[ERROR] Release checksum does not match provided checksum!"
  exit 1
fi
echo "[INFO] "

## 2. check signatures on the artifacts
echo "[INFO] Step 2. Check individual files"

for f in `find "${WORKDIR}" -type f | grep '\.\(zip\|rar\|jar\|war\)$'`
do
  n=`basename "$f"`
  if [ ! -f "$f.asc" ]; then
    echo "[ERROR] $n.asc NOT FOUND"
    exit 1
  elif gpg --verify "$f.asc" 2>/dev/null; then
    echo "[INFO] $n.asc is OK"
  else
    echo "[ERROR] $n.asc is NOT OK"
    exit 1
  fi

  for hash in md5 sha1
  do
    tp=`echo $hash | cut -c 1-3`
    if [ ! -f "$f.$tp" ]; then
      echo "[ERROR] $n.$tp NOT FOUND"
      exit 1
    else
      A="`cat "$f.$tp" 2>/dev/null`"
      B="`openssl "$hash" "$f" 2>/dev/null | sed 's/.*= *//' `"
      if [ $A = $B ]; then
        echo "[INFO] $n.$tp is OK"
      else
        echo "[ERROR] $n.$tp is NOT OK"
        exit 1
      fi
    fi
  done
done

## 3. check tag contents vs src archive contents
echo "[INFO] "
echo "[INFO] Step 3. Compare svn tag with src zip file contents"

echo "[INFO] Doing svn checkout, please wait..."
SVNTAGDIR="$WORKDIR/tag-svn/jackrabbit-oak-$VERSION"
svn --quiet export "https://svn.apache.org/repos/asf/jackrabbit/oak/tags/jackrabbit-oak-$VERSION" "$SVNTAGDIR"

echo "[INFO] Unzipping src zip file, please wait..."
ZIPTAG="$WORKDIR/tag-zip"
unzip -q "$WORKDIR/$VERSION/jackrabbit-oak-$VERSION-src.zip" -d "$ZIPTAG"
ZIPTAGDIR="$ZIPTAG/jackrabbit-oak-$VERSION"

echo "[INFO] Comparing sources, please wait..."
DIFFOUT=`diff -b -r "$SVNTAGDIR" "$ZIPTAGDIR"`
if [ -n "$DIFFOUT" ]
then
  echo "[ERROR] Found some differences!"
  echo "$DIFFOUT"
  exit 1
else
  echo "[INFO] No differences found."
fi
echo "[INFO] "

## 4. run the build with the pedantic profile to have the rat licence check enabled

echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] RUNNING MAVEN BUILD                                                     "
echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] "
echo "[INFO] Running maven build, please wait..."

cd "$ZIPTAGDIR"
if mvn package -Ppedantic > ../maven-output.txt; then
  echo "[INFO] Maven build OK"
else
  echo "[ERROR] Maven build NOT OK"
  exit 1
fi

echo "[INFO] "
echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] ALL CHECKS OK                                                           "
echo "[INFO] ------------------------------------------------------------------------"
exit 0
