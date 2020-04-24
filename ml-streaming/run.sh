docker kill ml-streaming
docker rm ml-streaming
docker rmi big-data_ml-streaming:latest
docker-compose -f ../docker-compose-3.yaml up -d