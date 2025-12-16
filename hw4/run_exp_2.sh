#!/bin/bash
for i in {1..5}; do
  docker exec -i clickhouse clickhouse-client -u admin --password admin --time --query "
SELECT t.region,
       countIf(gender = 1 AND dateDiff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
       countIf(gender = 0 AND dateDiff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
  FROM default.person_data_codec t
 WHERE t.is_marital = 1
   AND t.region IN ('80')
 GROUP BY t.region;
"
done
