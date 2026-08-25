clone https://github.com/conduktor/kafka-stack-docker-compose

run command 

  docker compose -f conduktor-kafka-single.yml up

  docker compose -f conduktor-kafka-single.yml down


get into kafka1 container

  docker exec -it kafka1 bash

