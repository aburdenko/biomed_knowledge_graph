#!/usr/bin/env bash

# Clear the logs
gcloud beta logging logs list | grep "functions" | awk '{print $1}' |  xargs -n 1 -P 5 gcloud logging logs delete 

# gcloud config set project raw-dai-redshift
pushd ../raw_files_functions
gsutil acl ch -p viewers-iomedical-graph-db:O gs://ncbi_papers
gsutil -m acl ch -p owners-biomedical-graph-db:O gs://ncbi_papers
gsutil -m acl ch -p editors-biomedical-graph-db:O gs://ncbi_papers
gsutil -m acl ch -p viewers-biomedical-graph-db:R gs://ncbi_papers
cp -R ../localpackage .
gcloud functions deploy ncbi_papers_raw --trigger-event google.storage.object.finalize --memory 2048MB --entry-point after_write --trigger-resource ncbi_papers --runtime python37
rm -rf ./localpackage
popd

pushd ../nlp_functions
cp -R ../localpackage .
gcloud functions deploy ncbi_papers_nlp --trigger-event google.storage.object.finalize --memory 2048MB --entry-point after_write --trigger-resource ncbi_papers --runtime python37
rm -rf ./localpackage
popd


