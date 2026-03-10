# Lab 01 — Download and Install Hadoop

**Date:** 7th January, 2026

## Objective

Set up a working Apache Hadoop environment on Ubuntu (WSL) for distributed storage and MapReduce processing.

## Software Installed

| Component | Version | Purpose |
|-----------|---------|---------|
| Java JDK | 11 | Required runtime for Hadoop |
| Apache Hadoop | 3.3.6 | Distributed storage (HDFS) + MapReduce |

## Environment Configuration

```bash
# Key environment variables (~/.bashrc)
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
```

## Hadoop Services

After installation, the following services should be running (verified via `jps`):

| Service | Role |
|---------|------|
| NameNode | Manages HDFS metadata |
| DataNode | Stores actual data blocks |
| SecondaryNameNode | Periodic checkpointing of NameNode |
| ResourceManager | Manages YARN resources |
| NodeManager | Manages containers on each node |

## Verification Commands

```bash
# Check Hadoop version
hadoop version

# Start HDFS and YARN
start-dfs.sh
start-yarn.sh

# Verify running services
jps

# Test HDFS
hdfs dfs -mkdir /test
hdfs dfs -ls /
```

## Web UIs

| Service | URL |
|---------|-----|
| HDFS NameNode | http://localhost:9870 |
| YARN ResourceManager | http://localhost:8088 |
