turbine-war
===========

Netflix Turbine Web Application with Nerve (airbnb) Instance Discovery through Zookeeper

##Running Locally##
###Dependencies###
Zookeeper
```
docker run -p 2181:2181 jplock/zookeeper
```
Note: this is note the docker container we use in the upper environments, but is fit for purpose locally
###Startup###
```
mvn tomcat7:run
```

