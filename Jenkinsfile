#!groovy​


@Library('jenkinsfile_library@v201709251440_361d122') _

buildWithMaven() {
    skipSonar = false
}
properties([
              buildDiscarder(logRotator(numToKeepStr: '20')),
            ])