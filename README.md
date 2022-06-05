# Git Streaming Analytics


## Initialize VMs and Swarm

### Step 1

Add your SSH key to cloud-cfg-openstack-client-ansible.txt. Then launch a small VM on openstack with this file. This VM will serve as the the opentack client and the docker manager node.

### Step 2

Login to the vm and authenticate with your Openstack credentials. Run the start_instances.py script and use the cloud-cfg-docker.txt user data file. The script will prompt you for various arguments.

### Step 3

Initialize docker swarm. Use the output from this command, which is a join link containing a secure token, on each node that was provisioned in step 2.

First iniitialize the manager node:
```
sudo docker swarm init
```

You will receive output. Copy and paste this into each of the nodes. Here is an example:
```
sudo docker swarm join --token AIFIEYQ(R&@3)#*Q(WRA(SFAOIRA@(RAJSFOJASFa
```

### Step 4

Label the nodes according to the various labels (LABEL_FROM_YML_FILE) inside the pulsar-stack.yml file. 

First, list the nodes:

```
sudo docker node ls
```

Then, add a label to each node. Use the NODE_ID from the previous output. For example:
```
sudo docker node update --label-add node=LABEL_FROM_YML_FILE NODE_ID
```

For example, to label a node as Bookkeeper 2 using the style from the yml file:
```
sudo docker node update --label-add node=b2 FA42qafaWEA2gh
```

## Initialize Pulsar Cluster 

From here on out you will be executing Pulsar commands on Docker containers. To do this you can use the following syntax.

"Login" to the container. Then run the commands shown in below steps
```
sudo docker exec -it CONTAINER_ID /bin/bash
```
or
```
sudo docker exec -it CONTAINER_ID /RUN/COMMAND/HERE/USING/PATH_FROM_CONTEXT_OF_DOCKER_CONTAINER
```


To get a container id:
```
sudo docker container ls
```



### Step 5

On any docker container, run the following command. Substitute PUBLIC_IP.

```
bin/pulsar initialize-cluster-metadata \
  --cluster pulsar-cluster-1 \
  --metadata-store zk:zookeeper1:2181,zookeeper2:2181,zookeeper3:2181 \
  --configuration-metadata-store zk:zookeeper1:2181,zookeeper2:2181,zookeeper3:2181 \
  --web-service-url http://PUBLIC_IP:8080 \
  --broker-service-url pulsar://broker1:6650
```

### Step 7

On each Bookkeeper container, run the following command.

```
nohup bin/pulsar bookie
```

### Step 8

On each Broker container, run the following command.

```
nohup bin/pulsar broker
```

You now have a usable Pulsar cluster and can submit any pulsar client to it.

## Pulsar Client

### Step 9

On the pulsar client...

1) run layer3_consumer1/2
2) run layer2_q134/q2
3) run layer1_producer
