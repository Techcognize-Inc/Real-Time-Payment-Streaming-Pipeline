pipeline {
    agent any

    stages {

        stage('Clone Repository') {
            steps {
                deleteDir()
                git branch: 'main', url: 'https://github.com/Techcognize-Inc/Real-Time-Payment-Streaming-Pipeline.git'
            }
        }

        stage('Run Tests') {
            steps {
                sh '''
                docker run --rm -v $PWD:/app -w /app python:3.10 bash -c "
                pip install --upgrade pip &&
                pip install pytest &&
                pytest tests
                "
                '''
            }
        }

        stage('Start Docker Infrastructure') {
            steps {
                sh 'docker compose up -d'
            }
        }

        stage('Verify Kafka Topic') {
            steps {
                sh 'docker exec kafka kafka-topics --list --bootstrap-server kafka:9092'
            }
        }

        stage('Run Producer') {
            steps {
                sh 'python airflow/dags/producer/payment_producer.py &'
            }
        }

        stage('Submit Flink Job') {
            steps {
                sh 'docker exec jobmanager flink run /opt/flink/usrlib/payment_fraud_detection.py'
            }
        }
    }
}