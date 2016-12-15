# Income Capture

This document records process of building the models for income capture at the account/customer level

## Pull the response data
The following sql pulls the data with a response column from EAP (hadoop) via hive, which means clicked or not.

### SQL

#### Create a table for September data at location/impression level
```sql
create table rawwebbehavior.qz90625_cfin_resp_sep_loc AS select
    CASE
    WHEN substr(a.acct_nbr,1,1) = '3' THEN
    trim(concat('00',a.acct_nbr))
    ELSE trim(concat('0',a.acct_nbr))
    END AS acct_num, a.acct_nbr, a.per_num, a.loc_id,
    CASE
    WHEN b.acct_nbr is NOT NULL THEN
    1
    ELSE 0
    END AS resp
FROM 
  (SELECT DISTINCT a.acct_nbr,
         6401 AS per_num,
         loc_id
  FROM default.com_ofr_event_0916 a
  INNER JOIN rawwebbehavior.rp18607_ao_mcell_062016_accts b
      ON a.acct_nbr=b.acct_nbr
          AND b.som_mcell
      BETWEEN 00
          AND 04
  WHERE substr(a.event_dte,1,2) < '19'
          AND population_segment != 'B2'
          AND pid NOT IN (520,521,522,524,525,526)
          AND substr(a.ofr_id,6,4) IN ('CFIN')
          AND a.ofr_acpt='Extended'
  UNION ALL
  SELECT DISTINCT a.acct_nbr,
         6401 AS per_num,
         loc_id
  FROM default.com_ofr_event_0916 a
  INNER JOIN rawwebbehavior.rp18607_ao_mcell_092016_accts b
      ON a.acct_nbr=b.acct_nbr
          AND b.som_mcell
      BETWEEN 00
          AND 04
  WHERE substr(a.event_dte,1,2) > '18'
          AND population_segment != 'B2'
          AND pid NOT IN (520,521,522,524,525,526)
          AND substr(a.ofr_id,6,4) IN ('CFIN')
          AND a.ofr_acpt='Extended') a left outer
JOIN 
  (SELECT DISTINCT a.acct_nbr,
         6401 AS per_num,
        loc_id
  FROM default.com_ofr_event_0916 a
  INNER JOIN rawwebbehavior.rp18607_ao_mcell_062016_accts b
      ON a.acct_nbr=b.acct_nbr
          AND b.som_mcell
      BETWEEN 00
          AND 04
  WHERE substr(a.event_dte,1,2) < '19'
          AND population_segment != 'B2'
          AND pid NOT IN (520,521,522,524,525,526)
          AND substr(a.ofr_id,6,4) IN ('CFIN')
          AND a.ofr_acpt='Clicked'
  UNION ALL
  SELECT DISTINCT a.acct_nbr,
         6401 AS per_num,
         loc_id
  FROM default.com_ofr_event_0916 a
  INNER JOIN rawwebbehavior.rp18607_ao_mcell_092016_accts b
      ON a.acct_nbr=b.acct_nbr
          AND b.som_mcell
      BETWEEN 00
          AND 04
  WHERE substr(a.event_dte,1,2) > '18'
          AND population_segment != 'B2'
          AND pid NOT IN (520,521,522,524,525,526)
          AND substr(a.ofr_id,6,4) IN ('CFIN')
          AND a.ofr_acpt='Clicked') b
    ON a.acct_nbr=b.acct_nbr
        AND a.per_num=b.per_num
        AND a.loc_id=b.loc_id
```

#### Create a table for September data at account level
```sql
create table rawwebbehavior.qz90625_cfin_resp_sep AS select
    CASE
    WHEN substr(a.acct_nbr,1,1) = '3' THEN
    trim(concat('00',a.acct_nbr))
    ELSE trim(concat('0',a.acct_nbr))
    END AS acct_num, a.acct_nbr, a.per_num,
    CASE
    WHEN b.acct_nbr is NOT NULL THEN
    1
    ELSE 0
    END AS resp
FROM 
  (SELECT DISTINCT a.acct_nbr,
         6401 AS per_num
  FROM default.com_ofr_event_0916 a
  INNER JOIN rawwebbehavior.rp18607_ao_mcell_062016_accts b
      ON a.acct_nbr=b.acct_nbr
          AND b.som_mcell
      BETWEEN 00
          AND 04
  WHERE substr(a.event_dte,1,2) < '19'
          AND population_segment != 'B2'
          AND loc_id IN ('INTR','OVLY','CALT1','CALT2','CNTX1','RAL1','RAL2')
          AND pid NOT IN (520,521,522,524,525,526)
          AND substr(a.ofr_id,6,4) IN ('CFIN')
          AND a.ofr_acpt='Extended'
  UNION ALL
  SELECT DISTINCT a.acct_nbr,
         6401 AS per_num
  FROM default.com_ofr_event_0916 a
  INNER JOIN rawwebbehavior.rp18607_ao_mcell_092016_accts b
      ON a.acct_nbr=b.acct_nbr
          AND b.som_mcell
      BETWEEN 00
          AND 04
  WHERE substr(a.event_dte,1,2) > '18'
          AND population_segment != 'B2'
          AND loc_id IN ('INTR','OVLY','CALT1','CALT2','CNTX1','RAL1','RAL2')
          AND pid NOT IN (520,521,522,524,525,526)
          AND substr(a.ofr_id,6,4) IN ('CFIN')
          AND a.ofr_acpt='Extended') a left outer
JOIN 
  (SELECT DISTINCT a.acct_nbr,
         6401 AS per_num
  FROM default.com_ofr_event_0916 a
  INNER JOIN rawwebbehavior.rp18607_ao_mcell_062016_accts b
      ON a.acct_nbr=b.acct_nbr
          AND b.som_mcell
      BETWEEN 00
          AND 04
  WHERE substr(a.event_dte,1,2) < '19'
          AND population_segment != 'B2'
          AND loc_id IN ('INTR','OVLY','CALT1','CALT2','CNTX1','RAL1','RAL2')
          AND pid NOT IN (520,521,522,524,525,526)
          AND substr(a.ofr_id,6,4) IN ('CFIN')
          AND a.ofr_acpt='Clicked'
  UNION ALL
  SELECT DISTINCT a.acct_nbr,
         6401 AS per_num
  FROM default.com_ofr_event_0916 a
  INNER JOIN rawwebbehavior.rp18607_ao_mcell_092016_accts b
      ON a.acct_nbr=b.acct_nbr
          AND b.som_mcell
      BETWEEN 00
          AND 04
  WHERE substr(a.event_dte,1,2) > '18'
          AND population_segment != 'B2'
          AND loc_id IN ('INTR','OVLY','CALT1','CALT2','CNTX1','RAL1','RAL2')
          AND pid NOT IN (520,521,522,524,525,526)
          AND substr(a.ofr_id,6,4) IN ('CFIN')
          AND a.ofr_acpt='Clicked') b
    ON a.acct_nbr=b.acct_nbr
        AND a.per_num=b.per_num
```
        
### Download data from hive to hdfs
```sh
insert overwrite directory '/user/qz90625/income_capture/sep' select * from rawwebbehavior.qz90625_cfin_resp_sep_loc where loc_id != 'SALT'
```

### Download data from hdfs to local dir on UAT
```sh
hadoop fs -getmerge /user/qz90625/income_capture/sep /data/1/rawwebbehavior/qz90625/income_capture/sep
```

### Transfer data from UAT to GRID (SAS)
```sh
scp test.txt qz90625@cagprod.nam.nsroot.net:~/income_capture
```

### Create tag table
1. copy the csv file to /tagfiles
   ```sh
   tagcp /path/csv_file
   ```
2. go to https://consumeranalyticsportal.citigroup.net
