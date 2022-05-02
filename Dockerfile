FROM debezium/server:1.9.2.Final AS build
WORKDIR /debezium
COPY . /debezium
# Start with a base image containing Java runtime 11
#FROM openjdk:11-jre-slim
#WORKDIR /
#COPY --from=build /debezium/ /debezium/
#WORKDIR /debezium

VOLUME ["/debezium/conf","/debezium/data"]

CMD ["/debezium/run.sh"]

EXPOSE 8080
