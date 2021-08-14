1. Explain the `Dockerfile` and `StageLevelSchedulingApp`
2. Install and start minikube (`minikube start`): https://minikube.sigs.k8s.io/docs/start/
3. Start the K8S dashboard: `minikube dashboard`
4. Build the project: `mvn clean install`
5. Build the Docker image: `docker build -t spark_kubernetes_concepts_demo .`
6. Publish the image to minikube: 
```
docker save spark_kubernetes_concepts_demo:latest > spark_kubernetes_concepts_demo.tar
minikube image load spark_kubernetes_concepts_demo.tar
# check if the the image was correctly loaded
# you should see "docker.io/library/spark_kubernetes_concepts_demo:latest"
minikube image ls
```
6. Create the demo namespace and a service account: 
```
kubectl create namespace spark-kubernetes-concepts-demo
kubectl config set-context --current --namespace=spark-kubernetes-concepts-demo
kubectl create serviceaccount spark-reader
kubectl create rolebinding spark-reader-role --clusterrole=view --serviceaccount=spark-kubernetes-concepts-demo:spark-reader
```
7. Download Apache Spark 3.1
8. Go to the directory with downloaded and extracted Apache Spark 3.1: 
`cd /home/bartosz/learning/apache_spark/envs/spark-3.1.1-bin-hadoop3.2`
and create the logger file:
```echo "log4j.rootCategory=DEBUG, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Set the default spark-shell log level to WARN. When running the spark-shell, the
# log level for this class is used to overwrite the root logger's log level, so that
# the user can have different defaults for the shell and regular Spark apps.
log4j.logger.org.apache.spark.repl.Main=DEBUG

# Settings to quiet third party logs DEBUG are too verbose
log4j.logger.org.sparkproject.jetty=WARN
log4j.logger.org.sparkproject.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR

# SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in SparkSQL with Hive support
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
" > conf/log4j.properties
```

9. Check the master address: `kubectl cluster-info` and export it as a variable:
`KUBERNETES_MASTER=https://172.17.0.2:8443`
10. Submit the job:
```
./bin/spark-submit \
    --master k8s://$KUBERNETES_MASTER \
    --deploy-mode cluster \
    --name kubernetes-concepts-demo \
    --class com.waitingforcode.StageLevelSchedulingApp \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-reader \
    --conf spark.kubernetes.namespace=spark-kubernetes-concepts-demo \
    --conf spark.kubernetes.container.image=spark_kubernetes_concepts_demo:latest \
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    local:////opt/application/spark_kubernetes_concepts-1.0-SNAPSHOT-jar-with-dependencies.jar
```

11. Check the outcome, it should fail because of the service account read-only permissions.

12. Create an editor role:
```
kubectl create serviceaccount spark-editor
kubectl create rolebinding spark-editor-role --clusterrole=edit --serviceaccount=spark-kubernetes-concepts-demo:spark-editor
```

13. Submit the application:
```
./bin/spark-submit \
    --master k8s://$KUBERNETES_MASTER \
    --deploy-mode cluster \
    --name kubernetes-concepts-demo \
    --class com.waitingforcode.StageLevelSchedulingApp \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-editor \
    --conf spark.kubernetes.namespace=spark-kubernetes-concepts-demo \
    --conf spark.kubernetes.container.image=spark_kubernetes_concepts_demo:latest \
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    local:////opt/application/spark_kubernetes_concepts-1.0-SNAPSHOT-jar-with-dependencies.jar
```


14. Get the driver pod id and read the info about the snapshot store from the logs:
* `kubectl logs $DRIVER_POD_NAME  | less` = look for "snapshot"

15. Create a Persistent Volume (inspired from https://jaceklaskowski.github.io/spark-kubernetes-book/demo/persistentvolumeclaims/#create-persistentvolumes):
```
minikube ssh
sudo mkdir /data/persistent-volume-spark-kubernetes-concepts-demo
```

```
kubectl delete pv --all
kubectl apply -f persistent_volume_definition.yaml
kubectl get pv
```

16. Submit the application with Persistent Volume Claim:
```
./bin/spark-submit \
    --master k8s://$KUBERNETES_MASTER \
    --deploy-mode cluster \
    --name kubernetes-concepts-demo \
    --class com.waitingforcode.StaticApp \
    --conf spark.executor.instances=2 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-editor \
    --conf spark.kubernetes.namespace=spark-kubernetes-concepts-demo \
    --conf spark.kubernetes.container.image=spark_kubernetes_concepts_demo:latest \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.kubernetes.context=minikube \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.pv.mount.path=/tmp/mounted-pvc \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.pv.options.claimName=OnDemand \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.pv.options.storageClass=manual \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.pv.options.readOnly=false \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.pv.options.sizeLimit=10Gi \
    --conf spark.kubernetes.executor.deleteOnTermination=false \
    local:////opt/application/spark_kubernetes_concepts-1.0-SNAPSHOT-jar-with-dependencies.jar
```

17. Get the driver pod id and read the info about the PVC from the logs:
* `kubectl logs $DRIVER_POD_NAME  | less` = look for "volume"

18. Clean-up:
```
kubectl config set-context --current --namespace=spark-kubernetes-concepts-demo
kubectl delete all --all
```
