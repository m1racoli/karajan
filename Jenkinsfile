#!/usr/bin/env groovy

podTemplate(
  cloud: 'woogikube',
  name: 'bit-ci',
  label: 'bit-ci',
  containers: [
    containerTemplate(name: 'airflow', image: 'wooga-docker.jfrog.io/bit/airflow/base:1.8.2', ttyEnabled: true, command: 'cat')
  ],
  volumes: [
    secretVolume(secretName: 'pypirc', mountPath: '/home/jenkins')
  ]
){
  node('bit-ci'){
    container('airflow'){
      stage('Build'){
        checkout scm
        sh 'make install'
      }
      stage('Test'){
        sh 'make test'
      }
      if (env.BRANCH_NAME == 'production') {
        stage('Release'){
          sh 'make release'
        }
      }
    }
  }
}
