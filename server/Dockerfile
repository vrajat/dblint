FROM openjdk:8-jre

# declare that the container listens on these ports
EXPOSE 8080
EXPOSE 8081

# standard command for starting a dropwizard service
ENTRYPOINT ["/usr/bin/java", "-jar", "/usr/lib/dblint/server.jar", "server", "/usr/lib/dblint/etc/dblint.yml"]

# add in project dependencies
ADD target/lib /usr/lib/dblint/lib

# add dropwizard config file - the server is configured to listen on ports 8080 (application port) and 8081 (admin port)
ADD target/config/dblint.yml /usr/lib/dblint/etc/dblint.yml

# add built dropwizard jar file - the JAR_FILE argument is configured in the dockerfile maven plugin
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/lib/dblint/server.jar