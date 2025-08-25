# debeziumMongodb

```bash
#build
docker build -t <name of your docker> -t gcr.io/<project_name>/<name of your docker> <path>
ex: docker build -t debezium_custom -t gcr.io/chrome-coast-348406/debezium_custom .

# load docker image to GCR
docker push gcr.io/chrome-coast-348406/debezium_custom

# run docker image from GCR
docker run gcr.io/chrome-coast-348406/debezium_custom

#deploy docker image to k8s
kubectl create deployment debezium --image=gcr.io/chrome-coast-348406/debezium_custom -r=3

#validate if k8s create successfully
kubectl get pods

# restart k8s if change any setting
kubectl rollout restart deployment debezium

#check how run the pods
kubectl describe pod debezium-7895d4f89-2cn7c

# validate the logs
kubectl logs -f debezium-7895d4f89-2cn7c

# increase or decrease replicaset of k8s
kubectl scale deployment debezium --replicas=2

# validate number of replica set in k8s
kubectl get rs

# check the logs
kubectl get pods -l app=debezium
```

## connect docker 
```bash
 docker run -it --entrypoint bash <docker_name>
 docker run -it --entrypoint bash debezium_custom
```

## connect pod:
```bash
kubectl exec -it <pod_name> -c <Pod_specification_name in k8s> -- /bin/bash
kubectl exec -it debezium-7b5cb9f6c4-6rkl2 -c debezium-custom-1 -- /bin/bash
```

## pubsub convention 
 ``` <mongodb.name>.<db_name>.<collection> ```

## execute mvn
```bash
 nohup mvn -P compile exec:java -Dexec.mainClass=com.mongodb.CdcPubsubToBq -Dexec.args='--project=chrome-coast-348406 --runner=DataflowRunner --tempGCSBQBucket=gs://dataflow_staging20220502/bqloads/ --tempGCSDataflowBucket=gs://dataflow_staging20220502/dataflowprocess/ --schema=video'
```

## Documentation
[debezium_mongodb](https://debezium.io/documentation/reference/1.9/connectors/mongodb.html#mongodb-property-collection-include-list)
