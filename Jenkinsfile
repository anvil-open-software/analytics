#!groovy​


@Library('jenkinsfile_library@v201709251440_361d122') _

buildWithMaven() {
    skipSonar = false
}

properties([buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '', daysToKeepStr: '30', numToKeepStr: '20')), gitLabConnection('gitlab'), [$class: 'RebuildSettings', autoRebuild: false, rebuildDisabled: false], [$class: 'ThrottleJobProperty', categories: [], limitOneJobWithMatchingParams: false, maxConcurrentPerNode: 0, maxConcurrentTotal: 0, paramsToUseForLimit: '', throttleEnabled: false, throttleOption: 'project'], pipelineTriggers([])])