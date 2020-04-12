# bigdata

Big data projects that include Java Spark submit application, Spark Streaming and Spark ML.

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
- Running the app
```
./run.sh
```
