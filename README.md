# bigdata projects

Big data projects that include Java Spark submit application, Spark Streaming and Spark ML.

All projects are running in a docker container and Java is used for their implementation.

The data that is being processed is uploaded to a HDFS that has one namenode and one datanode. Hadoop is also  running inside a docker container.

Data is processed with the Apache spark platform running in docker container. There is one master node and two worker nodes.

## Pre-reqs

To build and run this app locally you will need a few things:
- Install [Docker](https://docs.docker.com/engine/install/ubuntu/)

## Restore dataset

- Clone project
```
git clone git@github.com:JKostov/bigdata.git
```
- Create folder for the dataset
```
mkdir big-data
```
- Change directory
```
cd big-data
```
- Download the dataset
```
wget 'https://public.boxcloud.com/d/1/b1!hhQ1MT3fnKstfvZBWVvvkHFhTpV7mkLH2QahJsn_yai2r57fxzZ2l09QoqPwJFlrS87VeaL8tm828iB-eHdgUzK68BZ9D24iDMWzPMKvy1CSld6cRdViKriLwbujJQl0-yrgde6HVpjkpxc4-4xiOWF-XEVNyEac8nUpMhEp1surgk64smNRDMJ2_4rQV57QMIVbAVDJcuarYSZHt9HJ3ZKnxYcB2iOcf6a1yng_bN2XZt0XyaboAbSWzWifPgRTEpJ06J3HFPAOxKX8QJqmkaj-gMzLEEZsvIR52WDPoN7c9Nvy62S-_8eJqMN2fuIikxL-WBLIgV5V7a7iZIsoS57v8HhM2PgwAD_-Evs3TYGNlHrElzFg7Hz-dMdO9xnZfMA8fNlK3wD0IcK4ixGdvZVjSoOxZKccfHeOeTmBJjxPR-kLWJqbgKemHKIyzxxvk5NWysh4OkIH3SoLtp8bpdcyWidF1yibvOfkDL6OBrR-0Yrz7GYgs1oGtDBXu000s6r3y8TaJ6EaoNDTgH6EieKewzYwisRCP4RMfkLbBrQer9pBjBdB02ygw713w-MkpQgrh-3BJ-uDYECSxngkx3-IFqe8m-l46jXOtPjPCCL_6u5uaiD7HZlrftbY6elFVtiCJR6JnFHtyA3wVk8LUWwptfy9bF8Ip13qxmRnn1CHNVGYzrlGFOav-O9WiUFyTvg82gvDRhEM5de7ktmMthMuukE-HQJzHPHphFd_WdfRPYCmobReubx7h0vK0MuMStwiYBc679vODR5FkVOrZsoYhLX1sjdHVJF6wZhlSEayfTOWp17-6PXSm0ql0LbGB-Oo4xZVkE3cgnVV0y1Xadhr1jCOPwYuKeBX3w-We0nvdBqcH_kiP2gAnLt_nueWaLe9sMi52rPPEcxEj8Ma6ZknpJBhE0WnwSmcdc_ihfvhpQWaAFniAG8rOTeQkNWRaHuW8swb18m1sUbJVaGP6eHLG9uhwJmFUyesjN7CNlbw09wK-E874ceQoIkj5X6INl8zBvvoEX7Fbl6PX4I3pU1ag3pUdOLww4kFcNIvoyE6ztBQHW-FB1OiJSslpoVu_xqYbhC5PWN3Ve7eDlTs_RBozikoZmzw7vLo3JRlWpwX5K0wLwRQsqsdcAPBRfiq8pHoPMYHvRd-AtHAoJOpAjMGLgYi_e7RU7BZRxOtdTEb0txQ7Xi8IHJgMXkC96RkEp46xLs3ThNRpqOKy6qAV008BOdnXPTDTWl7ys12Ukqy-N6GZ1yuq4Hm3mXnBfvnmWO2hOiQVE9dDQlVGb-QdBmrAgJ5FpxB5k_cSETnQC6Wlow./download'
```
- If the command doesn't work download from this [link](https://osu.app.box.com/v/traffic-events-dec19)

- Extract the dataset
```
tar xf download
```
- Rename the extracted dataset
```
mv TrafficWeatherEvent_Aug16_June19_Publish.csv traffic-data.csv
```

## Running the project

```
docker-compose up
```

- Wait for all containers to start then run the next command
- pass argument as the project number 1 or 2 or 3
```
./run.sh
./run.sh 1
./run.sh 2
./run.sh 3
```

# Dataset
Contains traffic and weather events. This project is filtering and only using the traffic data. 
raffic event is a spatiotemporal entity, where such an entity is associated with location and time. Following is the description of available traffic event types:

- **Accident**: Refers to traffic accident which can involve one or more vehicles.
- **Broken-Vehicle**: Refers to the situation when there is one (or more) disabled vehicle(s) in a road.
- **Congestion**: Refers to the situation when the speed of traffic is lower than the expected speed.
- **Construction**: Refers to an on-going construction, maintenance, or re-paring project in a road.
- **Event**: Refers to the situations such as sport event, concert, and demonstration.
- **Lane-Blocked**: Refers to the cases when we have blocked lane(s) due to traffic or weather condition.
- **Flow-Incident**: Refers to all other types of traffic events. Examples are broken traffic light and animal in the road.

The data is stored in CSV file with the next columns:
1. **EventId**:	This is the identifier of a record
2. **Type**:	The type of an event; examples are accident and congestion.
3. **Severity**:	The severity of an event, wherever applicable. For a traffic event, severity is a number between 1 and 4, where 1 indicates the least impact on traffic (i.e., short delay as a result of the event) and 4 indicates a significant impact on traffic (i.e., long delay).
4. **TMC**:	Each traffic event has a Traffic Message Channel (TMC) code which provides a more detailed description on type of the event.
5. **Description**:	The natural language description of an event.
6. **StartTime (UTC)**:	The start time of an event in UTC time zone.
7. **EndTime (UTC)**:	The end time of an event in UTC time zone.
8. **TimeZone**:	The US-based timezone based on the location of an event (eastern, central, mountain, and pacific).
9. **LocationLat**:	The latitude in GPS coordinate.
10. **LocationLng**	The longitude in GPS coordinate.
11. **Distance (mi)**:	The length of the road extent affected by the event.
12. **AirportCode**:	The closest airport station to the location of a traffic event.
13. **Number**:	The street number in address record.
14. **Street**: The street name in address record.
15. **Side**:	The relative side of a street (R/L) in address record.
16. **City**:	The city in address record.
17. **County**:	The county in address record.
18. **State**:	The state in address record.
19. **ZipCode**:	The zipcode in address record.

80% of the traffic data are congestion events.

# Big-data 1 - Spark submit project

Implementation of the first project is in the `submit` folder, that is containg one Java Maven apllication. It is batch processing the whole dataset and it's extracting the next data:
- The first query is showing traffic event types in one city for the given timespan that occured more the N times.
- The second query is showing the distance of the affected road by the event in once city for the given timestamp.
- The third query is showing city that has max and min number of accidents in the given period.
- The last query is showing average congestion duration per city in the given period.

For running the first project you have to use the following command 
```
./run.sh 1
```
This script inside is using the `docker-compose-1.yaml` file.

# Big-data 2 - Spark streaming project

Implementation of the second project is in the `streaming-producer` and `streaming-consumer` folders. It is using Apache Kafka for publishing the data from the streaming-producer to the streaming-consumer application. Streaming-producer is Java Maven application that is reading the file on the HDFS line by line, filtering the traffic data and sending it to a kafka topic. Streaming-consumer is getting the data from the kafka topic and processing it. Also the processed data the streaming-consumer application is storing in **Mongo database**. 

- The application is finding the minumum, maximum and average congestion duration in a city.
- The processing of each bach that streaming-consumer got from the kafka topic is distrubuted among workers. Each batch is processed independently from the previous one, so for finding the min, max and avg value there had to be a state connecting the batches. For that purpose the application is using stateful processing and the current min, max and avg values and stored in a state. 

For running the second project you have to use the following command 
```
./run.sh 2
```
This script inside is using the `docker-compose-2.yaml` file.

# Big-data 3 - Spark ML project
