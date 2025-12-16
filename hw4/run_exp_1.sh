#!/bin/bash
for i in {1..5}; do
  docker exec -i clickhouse clickhouse-client -u admin --password admin --time --progress --query "
SELECT t.region,
       countIf(gender = 1 AND dateDiff('year', t.date_birth, now()) BETWEEN 20 AND 40) AS cnt_male,
       countIf(gender = 0 AND dateDiff('year', t.date_birth, now()) BETWEEN 18 AND 30) AS cnt_female
  FROM default.person_data_codec t
 WHERE t.date_birth BETWEEN toDate('2000-01-01') AND toDate('2000-01-31')
   AND t.region IN ('20', '25', '43', '59')
 GROUP BY t.region;
"
done
