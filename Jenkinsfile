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
                echo "Installing dependencies"
                pip3 install pytest faker kafka-python --break-system-packages

                echo "Running tests"
                pytest tests
                '''
            }
        }

        stage('Start Docker Infrastructure') {
            steps {
                sh '''
                echo "Starting Docker Infrastructure"
                docker-compose up -d zookeeper kafka flink-jobmanager flink-taskmanager postgres || true

                echo "Waiting for services to start..."
                sleep 120
                '''
            }
        }

        stage('Wait for Kafka') {
            steps {
                sh '''
                echo "Waiting for Kafka to be ready..."
                sleep 240
                '''
            }
        }

        stage('Run Producer') {
            steps {
                sh 'python3 airflow/dags/producer/payment_producer.py'
            }
        }

        stage('Verify Kafka Messages') {
            steps {
                sh '''
                echo "Reading Kafka messages..."
                docker exec kafka kafka-console-consumer \
                --bootstrap-server localhost:9092 \
                --topic payments_raw \
                --from-beginning \
                --max-messages 5
                '''
            }
        }

        stage('Verify Kafka Topic') {
            steps {
                sh '''
                docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
                '''
            }
        }

        stage('Submit Flink Job') {
            steps {
                sh '''
                echo "Copying Flink job..."
                docker cp flink_job/payment_stream_processor.py flink-jobmanager:/opt/flink/usrlib/payment_stream_processor.py

                echo "Submitting Flink job..."
                docker exec flink-jobmanager flink run -py /opt/flink/usrlib/payment_stream_processor.py
                '''
            }
        }

        stage('Create PostgreSQL Table') {
            steps {
                sh '''
                echo "Creating PostgreSQL aggregates table..."

                docker exec postgres psql -U postgres -d payments -c "
                CREATE TABLE IF NOT EXISTS payment_aggregates (
                    bank_code TEXT,
                    total_transactions INT,
                    failed_transactions INT,
                    success_rate DOUBLE PRECISION,
                    window_start TIMESTAMP,
                    window_end TIMESTAMP
                );
                "
                '''
            }
        }

        stage('Verify PostgreSQL Aggregates') {
            steps {
                sh '''
                echo "Waiting for Flink to process messages..."
                sleep 120

                echo "Fetching aggregates from PostgreSQL..."
                docker exec postgres psql -U postgres -d payments -c "SELECT * FROM payment_aggregates LIMIT 10;"
                '''
            }
        }

        stage('Monitoring Stack') {
            steps {
                echo 'Prometheus, Grafana, and Airflow are part of the monitoring and orchestration layer.'
                echo 'These services run independently in Docker and are skipped in the CI pipeline.'
            }
        }
}
}
