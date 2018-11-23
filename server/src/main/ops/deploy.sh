#!/bin/bash

set -e

cd $TRAVIS_BUILD_DIR/server
docker build -t us.gcr.io/${PROJECT_ID_PROD}/${DOCKER_IMAGE_NAME}:$TRAVIS_TAG .

echo $GCLOUD_SERVICE_KEY_PRD | base64 --decode -i > ${HOME}/gcloud-service-key.json
gcloud auth activate-service-account --key-file ${HOME}/gcloud-service-key.json

gcloud --quiet config set project $PROJECT_NAME_PROD
gcloud --quiet config set container/cluster $CLUSTER_NAME_PROD
gcloud --quiet config set compute/zone ${CLOUDSDK_COMPUTE_ZONE}
gcloud --quiet container clusters get-credentials $CLUSTER_NAME_PROD

gcloud auth configure-docker
gcloud docker push us.gcr.io/${PROJECT_NAME_PRD}/${DOCKER_IMAGE_NAME}:$TRAVIS_TAG

kubectl config view
kubectl config current-context

# kubectl set image deployment/${KUBE_DEPLOYMENT_NAME} ${KUBE_DEPLOYMENT_CONTAINER_NAME}=us.gcr.io/${PROJECT_NAME_PRD}/${DOCKER_IMAGE_NAME}:$TRAVIS_TAG
