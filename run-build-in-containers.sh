docker rm -f artio1
docker rm -f artio2
docker rm -f artio3
docker rm -f artio4
docker build -t artio .
docker run -d --cpus 1 --name artio1 artio
docker run -d --cpus 1 --name artio2 artio
docker run -d --cpus 1 --name artio3 artio
docker run -d --cpus 1 --name artio4 artio

echo
echo 'Execute the following to monitor the logs for test failures :'

echo 'docker logs artio1 -f | grep FAILED'
echo 'docker logs artio2 -f | grep FAILED'
echo 'docker logs artio3 -f | grep FAILED'
echo 'docker logs artio4 -f | grep FAILED'
