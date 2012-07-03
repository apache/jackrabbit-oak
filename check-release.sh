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

USERNAME=${1}
VERSION=${2}
SHA=${3}

if [ -z "$USERNAME" -o -z "$VERSION" -o -z "$SHA" ]
then
 echo "Usage: $0 <username> <version-number> <checksum> [temp-directory]"
 exit
fi

STAGING="http://people.apache.org/~$USERNAME/oak/$VERSION/"

WORKDIR=${4:-target/oak-staging-`date +%s`}
mkdir $WORKDIR -p -v

echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] DOWNLOAD STAGED REPOSITORY                                              "
echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] "

if [ `wget --help | grep "no-check-certificate" | wc -l` -eq 1 ]
then
  CHECK_SSL=--no-check-certificate
fi

wget $CHECK_SSL --wait 1 -nv -r -np "--reject=html,txt" -P "$WORKDIR" -nH "--cut-dirs=3" --ignore-length "${STAGING}"

echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] CHECK SIGNATURES AND DIGESTS                                            "
echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] "

## 1. check sha from release email against src.zip.sha file

downloaded_sha=$(cat `find $WORKDIR -type f | grep jackrabbit-oak-$VERSION-src.zip.sha`)
if [ "$SHA" = "$downloaded_sha" ]; then echo "[INFO] Step 1. Release checksum matches provided checksum."; else echo "[ERROR] Step 1. Release checksum does not match provided checksum!"; fi
echo "[INFO] "

## 2. check signatures on the artifacts
echo "[INFO] Step 2. Check individual files"

for f in `find ${WORKDIR} -type f | grep '\.\(zip\|rar\|jar\|war\)$'`
do
 echo "[INFO] $f"
 gpg --verify $f.asc 2>/dev/null
 if [ "$?" = "0" ]; then CHKSUM="GOOD"; else CHKSUM="BAD!!!!!!!!"; fi
 if [ ! -f "$f.asc" ]; then CHKSUM="----"; fi
 echo "gpg:  ${CHKSUM}"

 for hash in md5 sha1
 do
   tp=`echo $hash | cut -c 1-3`
   if [ ! -f "$f.$tp" ]
   then
     CHKSUM="----"
   else
     A="`cat $f.$tp 2>/dev/null`"
     B="`openssl $hash $f 2>/dev/null | sed 's/.*= *//' `"
     if [ "$A" = "$B" ]; then CHKSUM="GOOD (`cat $f.$tp`)"; else CHKSUM="BAD!! : $A not equal to $B"; fi
   fi
   echo "$tp : ${CHKSUM}"
 done
done

## 3. check tag contents vs src archive contents
echo "[INFO] "
echo "[INFO] Step 3. Check SVN Tag for version $VERSION with src zip file contents"

echo "[INFO] doing svn checkout, please wait..."
SVNTAGDIR="$WORKDIR/tag-svn/jackrabbit-oak-$VERSION"
svn --quiet export http://svn.apache.org/repos/asf/jackrabbit/oak/tags/jackrabbit-oak-$VERSION $SVNTAGDIR

echo "[INFO] unzipping src zip file, please wait..."
ZIPTAG="$WORKDIR/tag-zip"
unzip -q $WORKDIR/jackrabbit-oak-$VERSION-src.zip -d $ZIPTAG
ZIPTAGDIR="$ZIPTAG/jackrabbit-oak-$VERSION"

DIFFOUT=`diff -r $SVNTAGDIR $ZIPTAGDIR`
if [ -n "$DIFFOUT" ]
then
 echo "[ERROR] Found some differences!"
 echo "$DIFFOUT"
else
 echo "[INFO] No differences found."
fi

## 4. run the build with the pedantic profile to have the rat licence check enabled

echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] RUNNING MAVEN BUILD                                                     "
echo "[INFO] ------------------------------------------------------------------------"
echo "[INFO] "

cd "$ZIPTAGDIR"
mvn package -Ppedantic

