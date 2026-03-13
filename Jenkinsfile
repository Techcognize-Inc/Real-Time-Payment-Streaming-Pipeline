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
        python3 -m pip install --upgrade pip
        pip3 install pytest
        pytest tests
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
                docker exec kafka kafka-topics --list --bootstrap-server kafka:9092
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
                docker exec flink-jobmanager flink run -py /opt/flink/usrlib/payment_stream_processor.py
                '''
            }
        }

        stage('Prometheus Monitoring (Skipped)') {
            when {
                expression { false }
            }
            steps {
                echo 'Prometheus metrics collection is handled automatically via docker-compose.'
            }
        }

        stage('Grafana Dashboard (Skipped)') {
            when {
                expression { false }
            }
            steps {
                echo 'Grafana dashboards visualize Prometheus metrics and do not require Jenkins execution.'
            }
        }

        stage('Airflow Orchestration (Skipped)') {
            when {
                expression { false }
            }
            steps {
                echo 'Airflow DAGs orchestrate workflows but are not triggered by this Jenkins pipeline.'
            }
        }

    }
}