#!/usr/bin/env groovy

podTemplate(
  cloud: 'woogikube',
  name: 'karajan-ci',
  label: 'karajan-ci',
  containers: [
    containerTemplate(name: 'python', image: 'python:2.7-alpine', ttyEnabled: true, command: 'cat')
  ],
  volumes: [
    secretVolume(secretName: 'pypirc', mountPath: '/home/jenkins')
  ]
){
  node('karajan-ci'){
    container('python'){
      stage('Build'){
        steps {
          sh 'make install'
        }
      }
      stage('Test'){
        steps {
          sh 'make test'
        }
      }
    }
  }
}
