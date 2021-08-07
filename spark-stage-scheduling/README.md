1. Install minikube: https://minikube.sigs.k8s.io/docs/start/
2. Start the K8S dashboard: `minikube dashboard`
3. Build the project: `mvn clean install`
4. Build the Docker image: `docker build -t stage_level_scheduling_demo .`
5. Publish the image to minikube: 
```
docker save stage_level_scheduling_demo:latest > stage_level_scheduling_demo.tar
minikube image load stage_level_scheduling_demo.tar
# check if the the image was correctly loaded
# you should see "docker.io/library/stage_level_scheduling_demo:latest"
minikube image ls
```
6. Create the demo namespace and a service account: 
```
kubectl create namespace stage-level-scheduling-demo
kubectl config set-context --current --namespace=stage-level-scheduling-demo
kubectl create serviceaccount spark
kubectl create rolebinding spark-role --clusterrole=edit --serviceaccount=stage-level-scheduling-demo:spark --namespace=stage-level-scheduling-demo

```
7. Download Apache Spark 3.1
8. Go to the directory with downloaded and extracted Apache Spark 3.1: `cd /home/bartosz/learning/apache_spark/envs/spark-3.1.1-bin-hadoop3.2`
9. Check the master address: `kubectl cluster-info`
10. Submit the job:
```
./bin/spark-submit \
    --master k8s://https://172.17.0.3:8443 \
    --deploy-mode cluster \
    --name stage-levels-demo \
    --class com.waitingforcode.StageLevelSchedulingDemo \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=stage-level-scheduling-demo \
    --conf spark.kubernetes.container.image=stage_level_scheduling_demo:latest \
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    local:////opt/application/spark_stage_scheduling-1.0-SNAPSHOT-jar-with-dependencies.jar
```

11. Get the info about the resource profiles:
* `kubectl logs stage-levels-demo-dd7f5e7ac7e0b908-driver  | less` = look for "ResourceProfile"
* `kubectl port-forward stage-levels-demo-dd7f5e7ac7e0b908-driver 4040:4040` = check the added and removed executors
* http://127.0.0.1:33457/api/v1/namespaces/kubernetes-dashboard/services/http:kubernetes-dashboard:/proxy/#/overview?namespace=stage-level-scheduling-demo 
  = look for  spark-exec-resourceprofile-id labels
