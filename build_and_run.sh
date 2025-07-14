# build minIo image
docker build -t custom-minio -f src/docker/minio/Dockerfile .

docker run -d \
    -p 9000:9000 \
    -p 9001:9001 \
    -v ~/minio/data:/data \
    custom-minio
