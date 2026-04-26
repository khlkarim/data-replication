```bash
docker compose up -d # just spins up a rabbitmq instance

mvn package # builds the targets

# running a replica, you will be prompted to provide an ID for the replica
mvn exec:java -Dexec.mainClass="local.dev.replication.Replica"

# when you run a writer, it will ask you for text input
# anything you enter will be sent to the replicas as soon as you press enter
mvn exec:java -Dexec.mainClass="local.dev.replication.ClientWriter"

# when you run a reader, it will send a read a request to all the replicas 
# and log all the responds it will receive
mvn exec:java -Dexec.mainClass="local.dev.replication.ClientReader"
```
