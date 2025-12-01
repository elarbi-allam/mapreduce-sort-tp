#!/bin/bash

# Arrêt pour la sécurité
/usr/local/hadoop/sbin/stop-dfs.sh
/usr/local/hadoop/sbin/stop-yarn.sh

# 1. Lancement du NameNode en arrière-plan (sans daemon)
$HADOOP_HOME/bin/hdfs namenode &

# 2. Lancement du ResourceManager en arrière-plan
$HADOOP_HOME/bin/yarn resourcemanager &

# 3. Lancement du SecondaryNameNode (souvent une source d'instabilité)
$HADOOP_HOME/bin/hdfs secondarynamenode &

# Laissez les processus se stabiliser avant de continuer
sleep 15

# Vrifiez les processus
echo "--- JPS CHECK ---"
jps
echo "-----------------"