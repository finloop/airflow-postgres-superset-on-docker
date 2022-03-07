DOCKER_NETWORK = docker-hadoop_default
ENV_FILE = hadoop.env
current_version := 0.1
build:
	docker build -t finllop/hadoop-base:$(current_version) ./base
	docker build -t finloop/hadoop-datanode:$(current_version) ./datanode
	docker build -t finloop/hadoop-namenode:$(current_version) ./namenode
	docker build -t finloop/hadoop-historyserver:$(current_version) ./historyserver
	docker build -t finloop/hadoop-nodemanager:$(current_version) ./nodemanager
	docker build -t finloop/hadoop-resourcemanager:$(current_version) ./resourcemanager
