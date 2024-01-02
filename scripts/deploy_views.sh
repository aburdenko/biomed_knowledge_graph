#!/bin/bash
for i in `ls ../views/*vw*.sql`
do
    echo "refreshing $i ..."    
    v=$( cat ../views/$i )    
    
	bq query --use_legacy_sql=false \
        "$v"        

	echo "done."	

done
