docker network create --driver bridge ----subnet=172.27.0.0/16 --gateway=172.27.0.1 plant_network
cd Dockerfiles 
docker build -t python_server . 