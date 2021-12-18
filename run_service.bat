docker run -dit --name some-postgres -e POSTGRES_DB=plant_database -e POSTGRES_USER=user ^
	-e POSTGRES_PASSWORD=pass ^
	-e PGDATA=/var/lib/postgresql/data/pgdata -p 5432:5432 --name postgres_db -u 0 --network plant_network postgres
docker run -dit -e PGADMIN_DEFAULT_PASSWORD=pass -e PGADMIN_DEFAULT_EMAIL=mail@mail.ru --name pgadmin -p 80:80 -u 0 --network plant_network dpage/pgadmin4
docker run -dit --name=grafana -p 3000:3000 -u 0 --network plant_network grafana/grafana
docker run -dit --name python_container -p 8889:8888 ^
	-v .\python_server_docker:/home/task1/scripts ^
	-v .\Option_2\data:/home/task1/files ^
	-v .\python_server_docker_2:/home/task2/scripts ^
	-v .\Option_2\drive:/home/task2/files ^
	--network plant_network -u 0 python_server

docker exec -d python_container python /home/task1/scripts/db_init.py 
docker exec -d python_container python /home/task1/scripts/data_quality_handler.py 
docker exec -d python_container python /home/task2/scripts/db_init_task2.py 


start firefox localhost:8889 localhost:80 localhost:3000
