# Run Docker

To run kafka dev server on local docker. 

`$ docker-compose up -d`

To create topic: 

`$ docker exec -it docker-kafka_kafka_1  kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 3 --topic com.canyapan.sample.my-topic`

Documentation: https://github.com/bitnami/bitnami-docker-kafka