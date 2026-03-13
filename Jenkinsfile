pipeline {
    agent {
        docker {
            image 'python:3.10'
        }
    }

    environment {
        REPO_URL = 'https://github.com/Techcognize-Inc/Real-Time-Payment-Streaming-Pipeline.git'
    }

    stages {

        stage('Clone Repository') {
            steps {
                git branch: 'main', url: "${REPO_URL}"
            }
        }

        stage('Install Dependencies') {
            steps {
                sh '''
                python -m pip install --upgrade pip
                pip install -r requirements.txt
                '''
            }
        }

        stage('Run Tests') {
            steps {
                sh 'pytest'
            }
        }

        stage('Start Docker Containers') {
            steps {
                sh 'docker compose up -d'
            }
        }

        stage('Verify Kafka Topic') {
            steps {
                sh 'docker exec kafka kafka-topics --list --bootstrap-server kafka:9092'
            }
        }

        stage('Submit Flink Job') {
            steps {
                sh '''
                docker exec flink-jobmanager flink run -py /opt/flink/usrlib/payment_stream_processor.py
                '''
            }
        }

    }
}