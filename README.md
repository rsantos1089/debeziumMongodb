# debeziumMongodb

docker build -t <name of your docker> -t gcr.io/chrome-coast-348406/<name of your docker> <path>
docker build -t debezium_custom -t gcr.io/chrome-coast-348406/debezium_custom .
docker images
docker push gcr.io/chrome-coast-348406/debezium_custom
docker run gcr.io/chrome-coast-348406/debezium_custom
kubectl get pods
kubectl rollout restart deployment debezium
kubectl describe pod debezium-7895d4f89-2cn7c
kubectl logs -f debezium-7895d4f89-2cn7c

docker push gcr.io/debezium_custom

--connect docker : docker run -it --entrypoint bash <docker_name>
--connect docker : docker run -it --entrypoint bash debezium_custom

--connect pod: kubectl exec -it <pod_name> -c <Pod_specification_name in k8s> -- /bin/bash
--connect pod: kubectl exec -it debezium-7b5cb9f6c4-6rkl2 -c debezium-custom-1 -- /bin/bash

pubsub convention : <mongodb.name>.<db_name>.<collection>
