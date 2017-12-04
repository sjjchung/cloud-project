#!/bin/bash

apt-get update

# Install vim
apt-get install -y vim
apt-get install -y git

rm -f /home/vagrant/spark_mooc_version

# Set JAVA_HOME
java -version
echo '' >> /etc/profile
echo '# set JAVA_HOME' >> /etc/profile
echo 'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-i386' >> /etc/profile
export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-i386
echo "JAVA_HOME=${JAVA_HOME}"

# Provision Hadoop
pushd ~
echo "Getting Hadoop..."
wget -q 'http://apache.website-solution.net/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz'
mv ./hadoop-2.6.5.tar.gz /home/vagrant
pushd /home/vagrant
tar xfz hadoop-2.6.5.tar.gz
rm -f hadoop-2.6.5.tar.gz
cd hadoop-*

HADOOP_HOME=$(pwd)
echo '' >> /etc/profile
echo '# set HADOOP_HOME, HADOOP_PREFIX and PATH' >> /etc/profile
echo "export HADOOP_HOME=${HADOOP_HOME}" >> /etc/profile
echo "export HADOOP_PREFIX=${HADOOP_HOME}" >> /etc/profile
echo 'export PATH=$HADOOP_PREFIX/bin:$PATH' >> /etc/profile
echo "HADOOP_PREFIX=${HADOOP_HOME}"

# echo '' >> etc/hadoop/hadoop-env.sh
# echo "export JAVA_HOME=${JAVA_HOME}" >> etc/hadoop/hadoop-env.sh
# echo "export HADOOP_PREFIX=${HADOOP_HOME}" >> etc/hadoop/hadoop-env.sh

popd
popd
