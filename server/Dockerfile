FROM openjdk:11-alpine

# declare that the container listens on these ports
EXPOSE 8080
EXPOSE 8081

RUN addgroup -g 1001 -S appuser && adduser -u 1001 -S appuser -G appuser
RUN mkdir /app && chown -R appuser:appuser /app
USER appuser

# add in project dependencies
ADD target/lib /app/lib

# add dropwizard config file - the server is configured to listen on ports 8080 (application port) and 8081 (admin port)
ADD target/classes/dblint.yml /app/etc/dblint.yml

# https://github.com/docker-library/openjdk/issues/77
ADD target/classes/ld-musl-x86_64.path /etc

# add built dropwizard jar file - the JAR_FILE argument is configured in the dockerfile maven plugin
ADD target/mart-server.jar /app/mart-server.jar

# standard command for starting a dropwizard service
ENTRYPOINT ["/usr/bin/java", "-jar", "/app/mart-server.jar", "server", "/app/etc/dblint.yml"]
