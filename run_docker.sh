docker compose -f airflow/docker-compose.yaml rm -fs

# Uncomment next line if you run this script on MacOS
# export DOCKER_DEFAULT_PLATFORM=linux/amd64

docker compose -f airflow/docker-compose.yaml up -d
