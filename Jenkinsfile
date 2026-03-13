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
                sh '''
                docker-compose up -d
                sleep 20
                '''
            }
        }

        stage('Verify Kafka Topic') {
            steps {
                sh '''
                docker exec kafka kafka-topics \
                --bootstrap-server kafka:9092 \
                --describe \
                --topic payment_events || true
                '''
            }
        }

        stage('Generate Payment Events') {
            steps {
                sh '''
                python airflow/dags/producer/payment_producer.py &
                '''
            }
        }

        stage('Submit Flink Job') {
            steps {
                sh '''
                docker exec jobmanager flink run -py /opt/flink/usrlib/payment_stream_processor.py
                '''
            }
        }

        stage('Prometheus Monitoring (Skipped)') {
            steps {
                echo 'Prometheus monitoring configured but skipped in Jenkins pipeline'
            }
        }

        stage('Grafana Dashboard (Skipped)') {
            steps {
                echo 'Grafana dashboards handled outside Jenkins'
            }
        }

        stage('Airflow Orchestration (Skipped)') {
            steps {
                echo 'Airflow DAGs managed separately'
            }
        }

    }
}