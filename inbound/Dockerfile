FROM adoptopenjdk/openjdk13:alpine-slim

ENV VERTICLE_FILE target/inbound-1.0-SNAPSHOT-jar-with-dependencies.jar
ENV VERTICLE_HOME /usr/verticles
ENV PORT 8080

COPY $VERTICLE_FILE $VERTICLE_HOME/inbound.jar

WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["java -jar $VERTICLE_HOME/inbound.jar"]
