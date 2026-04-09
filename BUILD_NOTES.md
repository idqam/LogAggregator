
# Week 1

First I started building docker compose on day one to set up the services I would initially need: zookeeper, kafka, postgres, prometheus, grafana. 


When specifing kafka, I desiabled auto creation of topics since on auto it defaults to a replication factor of 1 and 1 partition. Keeping it like this would cause us to loose parallelism causing a throughput bottleneck. It would be fault intolerant as a single broker is not resistant to data lose. 

When doing:
kafka-topics \
  --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 1 
it gives us control over how we scale the consumer groups via partion-based parallelism. Control how durable our setup is and the retention policy. I explicitly create topics to control partitioning for parallel consumption.


* In event driven architectures, the source of truth for data and the source where we store derived data should be separate. 

The tradeoff is more operational overhead since now I need two Postgres containers, two init SQL files, two connection strings, and two volumes. But for this project its worth it and shows I understand the separation. In production these would just be two separate RDS instances, probably in different virtual private clouds VPCs. 