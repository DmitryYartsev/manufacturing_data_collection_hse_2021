# Manufacturing Data Collection \ HSE 2021
Homework for task 2
<br>To deploy server, you need to run docker containers </br>
0) git clone https://github.com/DmitryYartsev/manufacturing_data_collection_hse_2021.git
1) run create_artifacts.bat
<br>It will create some docker images and docker network</br>

2) run run_service.bat
<br>It will run 4 doccker containers:</br>
  &ensp;PostgreSQL (port - 5432)</br>
  &ensp;Grafana (port - 3000)</br>
  &ensp;Python (port - 8889)</br>
  &ensp;PgAdmin (port - 80)</br>

  3) PostgreSQL credention: 
  <br>dbname = 'plant_database' </br>
  user = 'user' </br>
  host = '172.27.0.2' </br>
  password = "pass" </br>
  port = 5432 </br>


4) For task_1 you should connect PgAdmin to PostgreSQL and find SQL command in path python_server_docker/sql_commands 

5) For task 2 you should connect Grafana to PostgreSQL and create dashboard from json temaplate Grafana_dashboard.json 

6) For stopping service run stop_server.bat

