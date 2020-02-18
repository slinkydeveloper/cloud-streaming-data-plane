FROM adoptopenjdk/openjdk13:alpine-slim

ENV VERTICLE_FILE target/runtime-1.0-SNAPSHOT-jar-with-dependencies.jar

ENV VERTICLE_HOME /usr/verticles

COPY $VERTICLE_FILE $VERTICLE_HOME/runtime.jar

WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["java -jar $VERTICLE_HOME/runtime.jar -Dorg.slf4j.simpleLogger.defaultLogLevel=debug"]
