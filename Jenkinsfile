#!/usr/bin/env groovy

podTemplate(
  cloud: 'woogikube',
  name: 'karajan-ci',
  label: 'karajan-ci',
  containers: [
    containerTemplate(name: 'airflow_base', image: 'wooga-docker.jfrog.io/bit/airflow_base:latest', ttyEnabled: true, command: 'cat')
  ],
  volumes: [
    secretVolume(secretName: 'pypirc', mountPath: '/home/jenkins')
  ]
){
  node('karajan-ci'){
    container('airflow_base'){
      stage('Build'){
        checkout scm
        sh 'make install'
      }
      stage('Test'){
        sh 'make test'
      }
    }
  }
}
