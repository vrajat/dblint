FROM openjdk:8-alpine

# declare that the container listens on these ports
EXPOSE 8080
EXPOSE 8081

# standard command for starting a dropwizard service
ENTRYPOINT ["/usr/bin/java", "-jar", "/usr/lib/dblint/mart-server.jar", "server", "/usr/lib/dblint/etc/dblint.yml"]

# add in project dependencies
ADD target/lib /usr/lib/dblint/lib

# add dropwizard config file - the server is configured to listen on ports 8080 (application port) and 8081 (admin port)
ADD target/classes/dblint.yml /usr/lib/dblint/etc/dblint.yml

# https://github.com/docker-library/openjdk/issues/77
ADD target/classes/ld-musl-x86_64.path /etc

# add built dropwizard jar file - the JAR_FILE argument is configured in the dockerfile maven plugin
ADD target/mart-server.jar /usr/lib/dblint/mart-server.jar