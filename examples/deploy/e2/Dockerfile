# E2's node image (chapter 17): a Java 21 runtime and the examples' jars, which `sbt examples/stageE2` puts in
# examples/target/e2/lib. Docker Compose builds it (docker-compose.yml); by hand, from the repository's root:
#   sbt examples/stageE2
#   docker build -f examples/deploy/e2/Dockerfile -t reactivecqrs-e2:1.0.0 examples/target/e2

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY lib /app/lib

# Pekko's port between nodes, and Pekko Management's (the health checks).
EXPOSE 17355 8558

# The node's settings come from the environment: NODE_HOST, SEED_NODES, DB_URL, DB_USER, DB_PASSWORD, APP_VERSION and
# SHOP_CONFIG (e2 or e2-kubernetes); see ClusterSetup.
ENTRYPOINT ["java", "-cp", "/app/lib/*", "io.reactivecqrs.examples.e2.ClusterSetup"]
