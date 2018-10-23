####Steps to run
1. build and run the containers
```
docker-compose build

docker-compose up -d
```
2. build app containers
```
docker build -t java .

docker run -d --name java java

docker run -d --net=demo_web --name java java

docker exec -it java sh
```
java -jar App.jar


3. Then re-run docker-compose:
```
docker-compose kill
docker-compose rm -f
docker-compose up -d
```

