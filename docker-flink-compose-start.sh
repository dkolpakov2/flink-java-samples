#!/usr/bin/env bash
export FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager"
printenv FLINK_PROPERTIES
# to print all envs:
# printenv
## already created once can be skipped:
docker network create flink-network
 ## result: 
	# 8cbf424057aacc72a357fb57aeb4be1bd8b2be43ae24c566d3aeef7fe6834489
	
	
## jobmanager
docker run \
    --rm \
    --name=jobmanager \
    --network flink-network \
    --publish 8081:8081 \
    --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
	--env TASK_MANAGER_NUMBER_OF_TASK_SLOTS="30" \
    flink:latest jobmanager
## taskmanager
##  taskmanager.numberOfTaskSlots="30"
docker run \
    --rm \
    --name=taskmanager \
    --network flink-network \
    --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
	--env TASK_MANAGER_NUMBER_OF_TASK_SLOTS="30" \
    flink:latest taskmanager	