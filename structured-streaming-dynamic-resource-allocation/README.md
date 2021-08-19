1. Explain the `Dockerfile` and `AutoscaledRateStreamReader`
2. Install and start minikube (`minikube start`): https://minikube.sigs.k8s.io/docs/start/
3. Start the K8S dashboard: `minikube dashboard`
4. Build the project: `mvn clean install`
5. Publish the image to minikube: 
```
docker build -t spark_structured_streaming_autoscaling .
docker save spark_structured_streaming_autoscaling:latest > spark_structured_streaming_autoscaling.tar
minikube image load spark_structured_streaming_autoscaling.tar
# check if the the image was correctly loaded
# you should see "docker.io/library/spark_structured_streaming_autoscaling:latest"
minikube image ls
```
6. Create the demo namespace and a service account: 
```
K8S_NAMESPACE=spark-structured-streaming-autoscaling-demo
kubectl create namespace $K8S_NAMESPACE
kubectl config set-context --current --namespace=$K8S_NAMESPACE
kubectl create serviceaccount spark-editor
kubectl create rolebinding spark-editor-role --clusterrole=edit --serviceaccount=$K8S_NAMESPACE:spark-editor
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
    --class com.waitingforcode.AutoscaledRateStreamReader \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-editor \
    --conf spark.kubernetes.namespace=$K8S_NAMESPACE \
    --conf spark.kubernetes.container.image=spark_structured_streaming_autoscaling:latest \
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    --conf spark.dynamicAllocation.schedulerBacklogTimeout=15s \
    --conf spark.dynamicAllocation.executorIdleTimeout=120s \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.kubernetes.executor.deleteOnTermination=false \
    local:////opt/application/structured_streaming_dynamic_resource_allocation-1.0-SNAPSHOT-jar-with-dependencies.jar
```

11. Get the driver pod id and read the info about the snapshot store from the logs:
* `kubectl logs $DRIVER_POD_NAME  | less` = look for "allocat"


12. Clean-up:
```
kubectl config set-context --current --namespace=$K8S_NAMESPACE
kubectl delete all --all
```
