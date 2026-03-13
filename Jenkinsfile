pipeline {
    agent any

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
                python3 -m pip install --upgrade pip
                pip3 install -r requirements.txt
                '''
            }
        }

        stage('Run Tests') {
            steps {
                sh 'pytest tests/'
            }
        }

        stage('Start Docker Pipeline') {
            steps {
                sh 'docker compose up -d'
            }
        }

        stage('Verify Kafka Topic') {
            steps {
                sh '''
                docker exec kafka kafka-topics \
                --bootstrap-server kafka:9092 \
                --list
                '''
            }
        }

        stage('Submit Flink Job') {
            steps {
                sh '''
                docker exec flink-jobmanager \
                flink run -py /opt/flink/usrlib/payment_stream_processor.py
                '''
            }
        }

    }

    post {
        success {
            echo 'CI Pipeline executed successfully'
        }

        failure {
            echo 'Pipeline failed. Check logs.'
        }
    }
}