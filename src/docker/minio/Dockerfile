# This image have free credentials feature
FROM minio/minio:RELEASE.2025-02-03T21-03-04Z-cpuv1


# Set variabels
ENV MINIO_ROOT_USER=minioadmin \
    MINIO_ROOT_PASSWORD=minioadmin

# Ports
EXPOSE 9000 9001

# Comands
CMD ["server", "/data", "--console-address", ":9001"]