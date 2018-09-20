#!groovy


@Library('jenkinsfile_library@v1.4.0') _

buildWithMaven() {
    skipSonar = false
}
properties([
        buildDiscarder(logRotator(numToKeepStr: '20')),
        ])