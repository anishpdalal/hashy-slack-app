cd core
python3 setup.py bdist_wheel
cd ../
cp -r core/dist backend
export DOCKER_DEFAULT_PLATFORM=
docker-compose build
cp -r core/dist indexer
cp -r core/dist scheduler
export DOCKER_DEFAULT_PLATFORM=linux/amd64
docker-compose -f docker-compose.prod.yml build