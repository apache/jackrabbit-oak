#!/usr/bin/env groovy
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// schedule oak-jcr:ut,oak-search-elastic,oak-segment-tar,oak-store-document first
// because they are longest running modules
def OAK_MODULES = 'oak-jcr:ut,oak-search-elastic,oak-segment-tar,oak-store-document,oak-api,oak-auth-external,oak-auth-ldap,oak-authorization-cug,oak-authorization-principalbased,oak-benchmarks,oak-benchmarks-elastic,oak-benchmarks-lucene,oak-benchmarks-solr,oak-blob,oak-blob-cloud,oak-blob-cloud-azure,oak-blob-plugins,oak-commons,oak-core,oak-core-spi,oak-examples,oak-exercise,oak-http,oak-it,oak-it-osgi,oak-jackrabbit-api,oak-jcr:it,oak-lucene,oak-pojosr,oak-query-spi,oak-run,oak-run-commons,oak-run-elastic,oak-search,oak-search-mt,oak-security-spi,oak-segment-aws,oak-segment-azure,oak-segment-remote,oak-solr-core,oak-solr-osgi,oak-standalone,oak-store-composite,oak-store-spi,oak-upgrade,oak-webapp'

properties([buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '', daysToKeepStr: '', numToKeepStr: '10'))])

def buildModule(moduleSpec) {
    def moduleName = ''
    def testOptions = '-PintegrationTesting'
    def idx = moduleSpec.indexOf(':') 
    if (idx == -1) {
        moduleName = moduleSpec
    } else {
        moduleName = moduleSpec.substring(0, idx)
        flags = moduleSpec.substring(idx + 1)
        if (flags == 'ut') {
            // unit tests only
            testOptions = '' 
        } else if (flags == 'it') {
            // integration tests only
            testOptions = '-PintegrationTesting -Dsurefire.skip.ut=true' 
        }
    }
    stage(moduleSpec) {
        node(label: 'ubuntu') {
            def JAVA_JDK_8=tool name: 'jdk_1.8_latest', type: 'hudson.model.JDK'
            def MAVEN_3_LATEST=tool name: 'maven_3_latest', type: 'hudson.tasks.Maven$MavenInstallation'
            def MAVEN_CMD = "mvn --batch-mode -Dmaven.repo.local=${env.HOME}/maven-repositories/${env.EXECUTOR_NUMBER}"
            def MONGODB_SUFFIX = sh(script: 'openssl rand -hex 4', returnStdout: true).trim()
            sh '''
            echo "Setting MAVEN_OPTS"
            echo "MAVEN_OPTS was ${MAVEN_OPTS}"
            export MAVEN_OPTS="-Xmx1536M"
            echo "MAVEN_OPTS now ${MAVEN_OPTS}"
            echo "Setting MAVEN_OPTS done"
            '''
            timeout(70) {
                checkout scm
                withEnv(["Path+JDK=$JAVA_JDK_8/bin","Path+MAVEN=$MAVEN_3_LATEST/bin","JAVA_HOME=$JAVA_JDK_8","MAVEN_OPTS=-Xmx1536M"]) {
                    sh '''
                    echo "MAVEN_OPTS is ${MAVEN_OPTS}"
                    '''
                    // clean all modules
                    sh "${MAVEN_CMD} -T 1C clean"
                    // build and install up to desired module
                    sh "${MAVEN_CMD} -Dbaseline.skip=true -Prat -T 1C install -DskipTests -pl :${moduleName} -am"
                    try {
                        sh "${MAVEN_CMD} ${testOptions} -DtrimStackTrace=false -Dnsfixtures=SEGMENT_TAR,DOCUMENT_NS -Dmongo.db=MongoMKDB-${MONGODB_SUFFIX} clean verify -pl :${moduleName}"
                    } finally {
                        archiveArtifacts(artifacts: '*/target/unit-tests.log', allowEmptyArchive: true)
                        junit(testResults: '*/target/surefire-reports/*.xml,*/target/failsafe-reports/*.xml', allowEmptyResults: true)
                    }
                }
            }
        }
    }
}

def stagesFor(modules) {
    def stages = [:]
    for (m in modules.tokenize(',')) {
        def module = m.trim()
        stages[module] = { buildModule(module) }
    }
    return stages
}

parallel stagesFor("${OAK_MODULES}")
