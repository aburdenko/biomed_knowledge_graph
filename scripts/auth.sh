#!/usr/bin/env bash
unset GOOGLE_APPLICATION_CREDENTIALS
#gcloud auth application-default login     
#gcloud auth list

gcloud auth login $1
gcloud auth list
gcloud config set project poc-ml-286316
