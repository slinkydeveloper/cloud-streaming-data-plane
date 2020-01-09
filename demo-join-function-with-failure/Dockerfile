FROM adoptopenjdk/openjdk13:alpine-slim

ENV VERTICLE_FILE target/demo-join-function-with-failure-1.0-SNAPSHOT-jar-with-dependencies.jar
ENV VERTICLE_HOME /usr/verticles
ENV PORT 8080

COPY $VERTICLE_FILE $VERTICLE_HOME/demo-join-function-with-failure.jar

WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["java -jar $VERTICLE_HOME/demo-join-function-with-failure.jar"]
