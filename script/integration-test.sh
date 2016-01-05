set -e

cd /home/wlngai/Workstation/Repo/tudelft-atlarge/granula/granula-modeller/openg/3.0-beta/analyzer
mvn clean install -DskipTests -q

cd /home/wlngai/Workstation/Repo/tudelft-atlarge/granula/granula-archiver
mvn clean install -DskipTests -q

cd /home/wlngai/Workstation/Repo/tudelft-atlarge/graphalytics
mvn clean install -Pgranula -DskipTests -q

cd /home/wlngai/Workstation/Repo/tudelft-atlarge/graphalytics-platforms-openg
mvn clean package  -DskipTests

cd /home/wlngai/Workstation/Data/openg/dist
rm -Rf graphalytics*

cp /home/wlngai/Workstation/Repo/tudelft-atlarge/graphalytics-platforms-openg/graphalytics-0.3-SNAPSHOT-openg-0.1-SNAPSHOT-bin.tar.gz .
tar -zxvf graphalytics-0.3-SNAPSHOT-openg-0.1-SNAPSHOT-bin.tar.gz
cp -r config graphalytics-0.3-SNAPSHOT-openg-0.1-SNAPSHOT

cd /home/wlngai/Workstation/Data/openg/dist/graphalytics-0.3-SNAPSHOT-openg-0.1-SNAPSHOT
filename=run$(date -d "today" +"%m%d-%H%M").log
./run-benchmark.sh &> $filename

cat $filename
