# flink-kafka-consumer-java-job

Flink Kafka consumer job for k8s operator.

## How to build
In order to build the project one needs maven and java.
Please make sure to set Flink version in
* `pom.xml` file with `flink.version` parameter
* `Dockerfile` file with `FROM` parameter
* `flink-kafka-consumer.yaml` file with `flinkVersion` parameter
```
mvn clean install

# Build the docker image into minikube
eval $(minikube docker-env)
docker build -t flink-kafka-consumer:latest .
```

## How to prepare minikube
```
minikube ssh
mkdir -p /tmp/flink
chmod 777 /tmp/flink
```

## How to deploy
```
kubectl apply -f flink-kafka-consumer.yaml
```

## How to delete
```
kubectl delete -f flink-kafka-consumer.yaml
```
