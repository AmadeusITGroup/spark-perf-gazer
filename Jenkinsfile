#!/usr/bin/env groovy
@Library(['pipeline-framework', 'pipeline-toolbox', 'faas']) _
import com.amadeus.iss.faas.databricks.DatabricksRestHelper
String dockerRepository = "dockerhub.rnd.amadeus.net:5005"
String dockerImageFullName = "dihdlk-app:v2"

String sbtOptions = "-Dsbt.log.noformat=true -Dsbt.override.build.repos=true -Dsbt.repository.config=.repositories"
def artifactoryRetries = 3

def isTechnicalCommit = false

def checkLastLog() {
  def lastLog = sh(returnStdout: true, script: 'git log -1 | tail -n 1').trim()
  echo 'lastLog=' + lastLog
  return lastLog.startsWith("[sbt-release]") || lastLog.startsWith("[doc]")
}

def configGit = {
  sh "git config --global credential.helper '!f() { sleep 1; echo \"username=${USERNAME}\npassword=${PASSWORD}\"; }; f'"
  sh 'git config --global user.name "swb2-izu-ATI"'
  sh 'git config --global user.email "swb2-izu-ATI@amadeus.com"'
  sh 'git config --list'
}

def fetchMasterTags = {
  // fetch all tags then keep only the ones merged on master
  sh "git fetch --tags origin +refs/heads/master:refs/remotes/origin/master"
  sh "git tag -d \$(git tag --no-merged origin/master)"
  sh "git tag -l"
}

def getProjectVersion() {
  return sh(script: "sbt -no-colors -error \"print utils/version \"", returnStdout: true).trim()
}

pipeline {
  agent {
    docker {
      image dockerRepository + '/' + dockerImageFullName
      args '-u root'
    }
  }

  stages {
    stage('Checkout') {
      steps {
        echo "Build branch ${env.BRANCH_NAME}"
        checkout scm
        script {
          isTechnicalCommit = checkLastLog()
        }
      }
    }

    // Get master merged tags and check if the branch commit comment prefixes match regex to be able to autoversion the application
    stage('Pre-release') {
      when {
        expression { !isTechnicalCommit }
      }

      steps {
        withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: 'IZ_USER', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD']]) {
          script {
            configGit()
            fetchMasterTags()
            sh "sbt ${sbtOptions} -batch 'show latestTag unreleasedCommits suggestedBump'"
          }
        }
      }
    }
    stage('Build') {
      when {
        expression { !isTechnicalCommit }
      }

      steps {
        script {
          withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: 'IZ_USER', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD']]) {
            sh "sbt ${sbtOptions} -batch clean compile"
          }
        }
      }
    }

    stage('Unit Tests') {
      when {
        expression { !isTechnicalCommit }
      }

      steps {
        sh "sbt ${sbtOptions} -batch coverageOn test coverageReport coverageAggregate"
      }
    }

    stage('Releasing to Artifactory') {
      when {
        expression { !isTechnicalCommit && env.BRANCH_NAME == 'master' }
      }

      options { retry(artifactoryRetries) }

      steps {
        withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: 'IZ_USER', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD']]) {
          script {
            configGit()
            fetchMasterTags()

            sh "git checkout ${env.BRANCH_NAME}"
            sh "git branch -u origin/${env.BRANCH_NAME}"
            sh "sbt ${sbtOptions} -batch 'release with-defaults skip-tests'"
            currentBuild.displayName = getProjectVersion()
          }
        }
      }
    }

    stage("Publish PR Snapshots") {
      when {
        expression { isPullRequest() }
      }
      stages{
        stage("Publish PR Snapshots to artifactory") {
          steps {
            withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: 'IZ_USER', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD']]) {
              script {
                def generatedVersion = "${getProjectVersion()}.PR${CHANGE_ID}.COMMIT${env.GIT_COMMIT.take(8)}"
                currentBuild.displayName = generatedVersion
                sh "sbt 'set every version := \"${generatedVersion}\"' ${sbtOptions} publish"
              }
            }
          }
        }
      }
    }
  }
}
