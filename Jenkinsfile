pipeline {
    agent any

    options {
        timestamps()
        timeout(time: 15, unit: 'MINUTES')
        disableConcurrentBuilds()
    }

    stages {

        stage('Checkout') {
            steps {
                git url: 'https://github.com/Techcognize-Inc/Real-Time-Payment-Streaming-Pipeline.git',
                    branch: 'main'
            }
        }

        stage('Start Docker') {
            steps {
                sh 'docker compose up -d'
            }
        }

        stage('Ready') {
            steps {
                echo '=================================='
                echo ' Airflow is ready!'
                echo ' Go to http://localhost:8085'
                echo ' Trigger the DAG to run the job'
                echo '=================================='
            }
        }
    }

    post {
        failure {
            sh 'docker compose down || true'
        }
    }
}
