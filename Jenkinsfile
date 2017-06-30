#!/usr/bin/env groovy

podTemplate(
  cloud: 'woogikube',
  name: 'karajan-ci',
  label: 'karajan-ci',
  containers: [
    containerTemplate(name: 'python', image: 'wooga/airflow_base', ttyEnabled: true, command: 'cat')
  ],
  volumes: [
    secretVolume(secretName: 'pypirc', mountPath: '/home/jenkins')
  ]
){
  node('karajan-ci'){
    container('python'){
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
