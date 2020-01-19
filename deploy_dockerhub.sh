#!/bin/sh

docker login -u $DOCKER_USER -p $DOCKER_PASS
printf "\nChecking Git Branch...\n"
if [ "$TRAVIS_BRANCH" = "master" ]; then
    TAG="latest"
else
    TAG="$TRAVIS_BRANCH"
fi
printf "\nGit branch -> %s" $TAG

printf "\nBuilding docker image..."
docker build -f Dockerfile -t encresearch/connector:$TAG .
printf "...OK"

printf "\nPushing docker image..."
docker push encresearch/connector:$TAG
printf "...OK"
