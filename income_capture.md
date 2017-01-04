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
```sql
create table rawwebbehavior.qz90625_cfin_resp_sep AS SELECT acct_num,
         max(resp) AS resp
FROM rawwebbehavior.qz90625_cfin_resp_sep_loc
WHERE loc_id!='SALT'
GROUP BY acct_num
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
scp /data/1/rawwebbehavior/qz90625/income_capture/sep qz90625@cagprod.nam.nsroot.net:~/income_capture
```

instead of the above 3 steps, what actually did is export/download the hive table (e.g. qz90625_cfin_resp_sep) as csv files to local csv and put them to GRID (/home/qz90625/income_capture/data/cfin_sep.csv)

### Create tag table
1. copy the csv file to /tagfiles
   ```sh
   tagcp /home/qz90625/income_capture/data/cfin_sep.csv
   ```
2. go to https://consumeranalyticsportal.citigroup.net

output in teradata DB p_bcd_v_I_consumer, p_tagdb_t

CFIN_AUG_TAG

CFIN_SEP_TAG

CFIN_OCT_TAG

CFIN_NOV_TAG

## Pull variables
### conventional 
### business logic (transaction level)
### machine learning (transaction level)

## IV criterion calculation
GRID: /gdm/coe/na_branded_cards/qz90625/income_capture

IV_calc.sas

Fineclass_Input.sas

output: IV_calc.lst, IV_CFIN_Sep16.xls

## Variable clusteing
VARCLUS2.sas

output: VARCLUS2.lst, summary_varclus_cfin.xls

## Variable selection
1. 7 steps to get temp2 from VARCLUS2.lst

2. Get selected variables
    UAT1 home dir 
    
    extract_var.groovy 
    
    input: IV_calc.lst, temp2 
    
    output: variable_selected 

3. Keep the selected variables 
    VarSelect.sas

## Flooring and Capping 
1. split variables into random small sets 
    UAT1 home dir
    
    random_var_split.groovy
    
    input: variable_selected
    
    output: random_var_split.csv
    
2. gen split and flooring&capping sas files
    UAT1 home dir
    
    random_var_gen_file.groovy
    
    input: random_var_split.csv
    
    output: split_var_x.sas, floor_cap_x.sas
    
3. back to GRID
    /gdm/coe/na_branded_cards/qz90625/income_capture
    
    mkdir cap
    
    nohup sas split_var_x.sas &
    
    nohup sas floor_cap_x.sas &
    
    cap/merge_final.sas to convert sas dataset to csv
    
    output: e.g. /gdm/coe/na_branded_cards/qz90625/income_capture/cap/merge_oct16_selected_final.csv
    
    transfer data from GRID to UAT1 /data/1/rawwebbehavior/qz90625/income_capture/
    
    transfer data to UAT1 to a H2O sever (R_Studio_H2O: bdgtproxy03i2d2) /data/1/namcards/
    
    put data to hdfs /user/qz90625/income_capture/
    
## Modeling

1. transfer csv files from GRID to hdfs on h2o node (bdgtproxy03i2d2 (R_Studio_H2O))
2. go to bdgtproxy03i2d2 (R_Studio_H2O)

    ```sh
    hadoop jar /opt/h2o/current/h2odriver.jar -nodes 3 -mapperXmx 6g -output OutputdirName
    ```
3. update ip and port in R code
    localH2O <- h2o.init(ip = "10.102.192.88", port = 54327) in the R code accordingly
