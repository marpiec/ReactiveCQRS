# E2's node image (chapter 17): the repository built with sbt, and E2 run with sbt.
#
# An example, not a production image: it holds the sources and sbt, and compiles at build time. A production image
# packages the application (sbt-native-packager, for one) and runs it with `java` only.
#
# Build it from the repository's root:
#   docker build -f examples/deploy/e2/Dockerfile -t reactivecqrs-e2 .

FROM sbtscala/scala-sbt:eclipse-temurin-21.0.8_9_1.11.7_2.13.18

WORKDIR /reactivecqrs
COPY . .
RUN sbt -batch examples/compile

# Pekko's port between nodes.
EXPOSE 17355

# The node's settings come from the environment: NODE_HOST, SEED_NODES, DB_URL, DB_USER, DB_PASSWORD, APP_VERSION and
# SHOP_CONFIG (e2 or e2-kubernetes); see ClusterSetup.
ENTRYPOINT ["sbt", "-batch", "examples/runMain io.reactivecqrs.examples.e2.ClusterSetup"]
