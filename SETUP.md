# SETUP — Global Fighting Guild Tournament PoC

End-to-end local setup. No Docker (as recommended). Everything runs on one
machine; each component is a standalone sbt project (`sbt run`).

## 0. Prerequisites

- **JDK 11 or 17** (JDK 11 is the smoothest with Spark 3.5; the `build.sbt`
  files already add the `--add-opens` flags needed for JDK 17)
- **sbt 1.10.x** — https://www.scala-sbt.org/download
- **Apache Kafka 3.7.x** (KRaft mode, no ZooKeeper)
- **PostgreSQL 14+**
- **Apache Hadoop 3.3.x** (HDFS, single-node/pseudo-distributed) — backs the data lake

Check versions:

```bash
java -version      # 11.x or 17.x
sbt --version
```

---

## 1. Kafka (distributed stream)

### Install (Linux/macOS, generic tarball)

```bash
cd ~
curl -O https://archive.apache.org/dist/kafka/3.7.1/kafka_2.13-3.7.1.tgz
tar -xzf kafka_2.13-3.7.1.tgz
cd kafka_2.13-3.7.1
```

> macOS with Homebrew: `brew install kafka` also works; adjust the `bin/` paths
> to the Homebrew-installed scripts.

### Format storage (KRaft) and start the broker

```bash
# one-time: initialise the KRaft metadata store
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/kraft/server.properties

# start the broker (keep this terminal open)
bin/kafka-server-start.sh config/kraft/server.properties
```

### Create the topic (new terminal)

```bash
cd ~/kafka_2.13-3.7.1

# positions: keyed by device_id -> 6 partitions to show scalability
bin/kafka-topics.sh --create --topic positions \
  --partitions 6 --replication-factor 1 \
  --bootstrap-server localhost:9092

# check
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

> There is no `alerts` Kafka topic: the alert detector (2) only writes alerts to
> PostgreSQL, and the alert handler (3), standing in for the mobile app, reads
> them straight from there.

Handy debug command:

```bash
# watch raw positions
bin/kafka-console-consumer.sh --topic positions --bootstrap-server localhost:9092
```

---

## 2. PostgreSQL (operational DB)

### Install

```bash
# Debian/Ubuntu
sudo apt-get update && sudo apt-get install -y postgresql
sudo service postgresql start

# macOS (Homebrew)
brew install postgresql@16 && brew services start postgresql@16
```

### Create the user and database

```bash
# create role 'gftt' with password 'gftt' and database 'gftt'
sudo -u postgres psql -c "CREATE USER gftt WITH PASSWORD 'gftt';"
sudo -u postgres psql -c "CREATE DATABASE gftt OWNER gftt;"
```

> On macOS/Homebrew there is usually no `postgres` OS user; use instead:
> `psql -d postgres -c "CREATE USER gftt WITH PASSWORD 'gftt';"` etc.

### Create the tables

From the project root:

```bash
PGPASSWORD=gftt psql -h localhost -U gftt -d gftt -f sql/init.sql
```

Verify:

```bash
PGPASSWORD=gftt psql -h localhost -U gftt -d gftt -c "\dt"
```

---

## 3. Data lake (distributed storage)

The data lake is backed by **HDFS**, shared by all components (Spark ships with
the Hadoop client needed to talk to `hdfs://`, no extra dependency required).

### Install (single-node / pseudo-distributed)

```bash
cd ~
curl -O https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
cd hadoop-3.3.6

export HADOOP_HOME=~/hadoop-3.3.6
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
```

> macOS with Homebrew: `brew install hadoop` also works; adjust paths to the
> Homebrew-installed layout.

Configure `etc/hadoop/core-site.xml` with the namenode address:

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

### Format the namenode and start HDFS

```bash
# one-time: format the namenode
bin/hdfs namenode -format
```

`sbin/start-dfs.sh` starts the daemons over SSH — even for a single local node —
so it fails with `Connection refused` unless an SSH server is running and
passwordless `ssh localhost` is set up. For a single-machine PoC, skip SSH
entirely and start each daemon directly instead:

```bash
# start the namenode, datanode and secondary namenode without SSH
bin/hdfs --daemon start namenode
bin/hdfs --daemon start datanode
bin/hdfs --daemon start secondarynamenode

# create the shared lake directory
bin/hdfs dfs -mkdir -p /gftt/datalake
```

> Prefer `start-dfs.sh`? Install an SSH server (`sudo apt-get install -y
> openssh-server`, `sudo service ssh start`) and set up passwordless login:
> `ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && cat ~/.ssh/id_rsa.pub >>
> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys`, then retry.

Verify:

```bash
bin/hdfs dfs -ls /gftt
```

To stop later: `bin/hdfs --daemon stop namenode` (same for `datanode` /
`secondarynamenode`), or `sbin/stop-dfs.sh` if you went the SSH route.

The default `DATA_LAKE` is `hdfs://localhost:9000/gftt/datalake`; it holds
`bronze/`, `silver/`, `gold/` and `_checkpoints/`. To use S3 instead, set
`DATA_LAKE=s3a://your-bucket/gftt` and add the `hadoop-aws` dependency + AWS
credentials.

---

## 4. Environment variables (defaults shown)

All components read these; the defaults already match a local setup, so you can
skip this unless you change ports/paths.

```bash
export KAFKA_BOOTSTRAP=localhost:9092
export POSITIONS_TOPIC=positions
export DATA_LAKE=hdfs://localhost:9000/gftt/datalake   # single shared lake on HDFS
export PG_URL=jdbc:postgresql://localhost:5432/gftt
export PG_USER=gftt
export PG_PASSWORD=gftt

# simulator tuning
export NB_DEVICES=40         # number of bracelets
export INTERVAL_MS=15000     # emit every 15 s (per the subject); lower it for a faster demo
export ALERT_DISTANCE_M=50   # fight triggers under 50 m
export ALERT_POLL_MS=2000    # alert handler (mobile app stand-in): DB polling interval
```

---

## 5. Run order

Run each component in its own terminal, from its own folder. Keep the streaming
ones (1, 2, 3, 4) running; run the batch ones (8, 5, 6, 7) on demand.

```bash
# Terminal A — start producing data
cd 1-bracelet-simulator && sbt run

# Terminal B — store everything raw into the Bronze lake
cd 4-bronze-ingestion && sbt run

# Terminal C — detect fights, write alerts to PostgreSQL
cd 2-alert-detector && sbt run

# Terminal D — mobile app stand-in: polls PostgreSQL alerts, push simulation + combat_results
cd 3-alert-handler && sbt run
```

Let it run for a couple of minutes so data accumulates. Then stop the streaming
components (Ctrl-C in terminals A–D) and run the batch stages (these finish and
exit). The weekly archive (8) moves `alerts` + `combat_results` from PostgreSQL
into the Bronze lake and purges the DB, so run it **before** the ETLs:

```bash
# Optional but recommended: check PostgreSQL BEFORE it is archived + purged
PGPASSWORD=gftt psql -h localhost -U gftt -d gftt \
  -c "SELECT count(*) FROM alerts;" \
  -c "SELECT winner_faction, count(*) FROM combat_results GROUP BY 1;"

cd 8-weekly-archive && sbt run  # PostgreSQL -> Bronze (Avro) + purge DB
cd 5-silver-etl     && sbt run  # Bronze -> Silver (positions parsed, combats/alerts curated)
cd 6-gold-etl       && sbt run  # Silver -> Gold (+ regenerates dashboard/data.js)
cd 7-analytics      && sbt run  # prints the 4 answers (reads the lake)
```

### Dashboard (personal part)

`6-gold-etl` regenerates `dashboard/data.js`. Serve the folder over HTTP and open
it in a browser:

```bash
cd dashboard && python3 -m http.server 8000
# then open http://localhost:8000
```

> First `sbt run` per component downloads dependencies (Spark is ~300 MB) and is
> slow; later runs are fast.

---

## 6. Cleanup / reset

```bash
# wipe the HDFS lake + Spark checkpoints (forces reprocessing from scratch)
bin/hdfs dfs -rm -r /gftt/datalake
bin/hdfs dfs -mkdir -p /gftt/datalake

# empty the tables
PGPASSWORD=gftt psql -h localhost -U gftt -d gftt \
  -c "TRUNCATE alerts, combat_results;"

# reset the Kafka topic
cd ~/kafka_2.13-3.7.1
bin/kafka-topics.sh --delete --topic positions --bootstrap-server localhost:9092
```

---

## 7. Notes on scalability (for the presentation)

- **Kafka**: `positions` keyed by `device_id` over 6 partitions → consumers scale
  horizontally by adding instances to the same consumer group.
- **Spark**: `local[*]` uses all local cores. On a real cluster, mark the Spark
  deps as `% "provided"`, build a fat jar (`sbt assembly` with `sbt-assembly`)
  and submit with `spark-submit --master yarn ...` — no code change needed.
- **Data lake**: already backed by HDFS, so it's distributed storage from the
  start; add datanodes to scale capacity, or swap `hdfs://` for `s3a://` and the
  same jobs run unchanged.
- Each of the 7 components starts/stops independently, so the pipeline is fully
  decoupled.
