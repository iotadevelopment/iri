#!/bin/bash

git pull
mvn clean compile
mvn package
systemctl stop iota
mv ./target/iri-1.5.1.jar /home/iota/node/iri-1.5.1.jar
chown iota:iota /home/iota/node/iri-1.5.1.jar
systemctl start iota
sudo journalctl -u iota -f