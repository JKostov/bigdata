docker kill streaming-consumer
docker rm streaming-consumer
docker rmi big-data_streaming-consumer:latest
docker-compose -f ../docker-compose-2.yaml up -d