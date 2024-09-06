 # ENHERTU Timeline Logic

This file consists of two parts: the firts part describes briefly that how the cohort and the relevant data is extracted, and the second part describes how the patient timelines have been built. All the coding is done with PySpark. 

## Cohort and data extraction logic

The cohort consits of patients that have had ENHERTU after the first breast cancer diagnosis.

```python
wanted_dg_uranus = ["person_id", "diagnosis_code", "reported_start_time", "contact_start_time"]
wanted_dg_apotti = ["person_id", "diagnosis_code", "reported_start_time", "contact_start_time"]

wanted_pat = ["person_id", "local_patient_id", "electronic_patient_id", "other_patient_id", "gender_description", "birth_date", "death_date"]

#Breast cancer diagnosis
cond_bc = (F.col("diagnosis_code").rlike(dg_bc))
#Time condition
cond_dg_time = (F.col("contact_start_time").between(start, end))

#Combine the relevant diagnosis data sources and filter
diagnosis_cohort_tmp = (uranus_diagnosis
                    .select(wanted_dg_uranus)
                    .union(
                      apotti_diagnosis
                      .selectExpr(wanted_dg_apotti)
                    )
                    .join(df_removal_list, on="person_id", how="leftanti")
                    .join(client.select(wanted_pat), on="person_id", how="left")
                    .filter(cond_bc & cond_dg_time)
                   )

#Window condiotions for the diagnosis contact/repoti time
wind_first_bc_dg_reported = Window.partitionBy("person_id").orderBy(F.col("reported_start_time").asc_nulls_last())
wind_first_bc_dg_contact = Window.partitionBy("person_id").orderBy(F.col("contact_start_time").asc_nulls_last())


first_bc_dg_hus_reported = (diagnosis_cohort_tmp
                              .withColumn("row", F.row_number().over(wind_first_bc_dg_reported))
                              .filter(F.col("row") == F.lit(1))
                              .select("person_id", "reported_start_time", "diagnosis_code"))
first_bc_dg_hus_contact = (diagnosis_cohort_tmp
                            .withColumn("row", F.row_number().over(wind_first_bc_dg_contact))
                            .filter(F.col("row") == F.lit(1))
                            .select("person_id", "contact_start_time", "diagnosis_code"))

#Find and select the first reported breast cancer diagnosis time or the first contact time with the breast cacner diagnose (select the earlier time)
diagnosis_cohort = (first_bc_dg_hus_reported
                    .join(first_bc_dg_hus_contact, on=["person_id", "diagnosis_code"], how="full")
                    .selectExpr("person_id", "reported_start_time as first_reported_start_time", "contact_start_time as first_contact_start_time", "diagnosis_code as first_breast_cancer_dg")
                    .withColumn("first_bc_start_time", F.when((F.col("first_reported_start_time") <= F.col("first_contact_start_time")) & (F.col("first_reported_start_time").isNotNull()), (F.col("first_reported_start_time"))).otherwise(F.col("first_contact_start_time")))
                    )
```

When one has combined all the relevant drug administration tables (not shown here), then the cohort can be filtered by taking the patients that have ENHERTU drug administrations after the breast cancer diagnose.

```python
#Enhertu administrations
cond_atc_enhertu = F.col("atc_code") == "L01FD04"
cond_trade_name_enhertu = F.col("trade_name").rlike("(?i)ENHERTU|trastuzumab.*deruxtecan")
cond_substance_enhertu = F.col("active_substance").rlike("(?i)trastuzumab.*deruxtecan")

#Drug given after the breast cancer diagnose
cond_time = F.col("start_time") > F.col("first_contact_start_time")

#Filter the results
enhertu = (medication_administration_records
           .join(diagnosis_cohort, on="person_id", how="inner")
           .filter(cond_time & (cond_atc_enhertu | cond_trade_name_enhertu | cond_substance_enhertu))
           )

```
After this, we are able to collect all the relevant data for the cohort:
* Drug administrations
* Drug orders
* Demographics
* Pathologies
* Labs (main interest has been the antigen test results)
* Procedures (incl. breast cancer surgeries)
* Diagnosis
* Texts (main interest has been the texts mentioning metastatis)


## Timeline logic
The timeline is combined from the cohort's data that is extracted above. The idea is that ones the relevant events have decided, they are then chronologically ordered for each patient. In this case, the relevant events are:

* Drug administrations of the cancer drugs (_F.col("atc_code").rlike("^L0[12]")_)
* Start and ending days of the cancer drugs from the drug orders
* Cancer pathologies (_F.col("diagnose").rlike("(?i)Carcinoma")_)
* Antigen test results from the labs (_F.col("test_longname").isin(["S -CA 15-3 Antigen", "S -CA 12-5 Antigen", "S -CA15-3 Antigen, comparison method", "S -Carcinoembryonic Antigen, comparison method", 
"S -CA 19-9 Antigen"])_)
* Breast cancer surgeries (_(^HAC.*)|(^HAB.*)|(^HAF.*)_)
* Cancer diagnosis (_C\d{2}_)
* Texts mentioning metastatis


 ```python
#Windows for ordering
wind_dg = Window().partitionBy("social_security_number", "diagnosis_code").orderBy(F.col("reported_start_time").asc())
wind_dg_statement = Window().partitionBy("social_security_number", "metastasis_diagnosis_codes").orderBy(F.col("event_time").asc())

#Cancer drug conditions
cond_atc_enhertu = F.col("atc_code") == "LO1FD04"
cond_cancer_drug_atc = F.col("atc_code").rlike("^L0[12]")
cond_cancer_drug = (cond_cancer_drug_atc)
cond_start_date_notnull = F.col("start_time").isNotNull()
cond_end_date_notnull = F.col("end_time").isNotNull()

#Cancer pathology condition
cond_carcinoma = F.col("diagnose").rlike("(?i)Carcinoma")

#Adding some missing ATC codes
medications_tmp = (df_dict["medications"]
                .withColumn("active_substance", F.lower("active_substance"))
                .withColumn("atc_code", 
                            F.when(F.col("active_substance")=="palbociclib", "L01EF01")
                            .when(F.col("active_substance")=="trastuzumab", "L01FD01")
                            .when(F.col("active_substance")=="trastuzumab emtansine", "L01FD03")
                            .when(F.col("active_substance")=="doxorubicin hydrochloride", "L01DB01")
                            .otherwise(F.col("atc_code"))
                            )
                )

#Selecting one active substance per ATC code
atc_active_substance = (medications_tmp
                   .filter(cond_cancer_drug_atc)
                   .select("atc_code", "active_substance")
                   .distinct()
                   .groupBy("atc_code")
                   .agg(F.first("active_substance").alias("active_substance"))
                   )

#pathology events
time_pat = (df_dict["pathology_dg"]
            .filter(cond_carcinoma)
            .withColumn("event", F.lit("pathology"))
            .withColumn("date", F.to_date("arrived"))
            .withColumn("other", F.concat(F.col("diagnose"), F.lit(", "), F.col("organ")))
            .selectExpr("social_security_number", "date", "event", "pathology_samplenumber as id", "other")
            .distinct()
            )

#Breast cancer operation events
time_bc_op = (df_dict["surgery"]
              .filter(F.col("breast_cancer_surgery") == True)
              .withColumn("event", F.lit("breast cancer surgery"))
              .withColumn("other", F.lit(None))
              .withColumn("id", F.lit(None))
              .selectExpr("social_security_number", "operation_date as date", "event", "id", "other")
              .distinct()
              )


#Drug administration events
time_medication_administration = (df_dict["medications_administration_records"]
                  .withColumn("atc_code", 
                            F.when(F.col("trade_name")=="TRASTUZUMAB-DERUXTECAN IV INFUSION", "L01XC41")
                            .when(F.col("trade_name")=="TRASTUZUMAB IV INF 250 ML", "L01FD01")
                            .when(F.col("trade_name")=="SACITUZUMAB GOVITEKAN IV INFUSION 250 ML", "L01FX17")
                            .when(F.col("active_substance")=="trastuzumab emtansine", "L01FD03")
                            .when(F.col("active_substance")=="doxorubicin hydrochloride", "L01DB01")
                            .otherwise(F.col("atc_code"))
                            )
                   .filter(cond_cancer_drug)
                   .withColumn("date", F.to_date("start_time"))
                   .withColumn("id", F.lit(None))
                   .withColumn("trade_name", F.when(F.col("trade_name").isNull(), F.col("active_substance")).otherwise(F.col("trade_name")))
                   .selectExpr("social_security_number", "date", "trade_name as event", "id", "atc_code as other")
                   .distinct()
                   )

#Cancer drug start dates from drug orders
time_medication_min = (medications_tmp
                  .filter((cond_cancer_drug_atc) & cond_start_date_notnull)
                  .withColumn("start_date", F.to_date("start_time"))
                  .groupBy("social_security_number", "atc_code")
                  .agg(F.min("start_date").alias("date"))
                  .withColumn("id", F.lit(None))
                  .join(atc_active_substance, "atc_code", "inner")
                  .withColumn("event", F.concat(F.lit("start "), F.col("active_substance")))
                  .selectExpr("social_security_number", "date", "event", "id", "atc_code as other")
                  )

#Cancer drug end dates from drug orders
time_medication_max = (medications_tmp
                  .filter((cond_cancer_drug_atc) & cond_end_date_notnull)
                  .withColumn("end_date", F.to_date("end_time"))
                  .groupBy("social_security_number", "atc_code")
                  .agg(F.max("end_date").alias("date"))
                  .withColumn("id", F.lit(None))
                  .join(atc_active_substance, "atc_code", "inner")
                  .withColumn("event", F.concat(F.lit("end "), F.col("active_substance")))
                  .selectExpr("social_security_number", "date", "event", "id", "atc_code as other")
                  )

#Cencer diagnosis
time_diagnosis = (df_dict["diagnoses"]
                  .withColumn("row_num", F.row_number().over(wind_dg))
                  .filter((F.col("row_num")==F.lit(1)) & (F.col("cancer_diagnosis")==True))
                  .withColumn("date", F.to_date("reported_start_time"))
                  .withColumn("id", F.lit(None))
                  .selectExpr("social_security_number", "date", "diagnosis_code as event", "id", "diagnosis_description as other")
                  .distinct()
                  )

#Antigen results
time_lab = (df_dict["mutilab"]
              .filter(F.col("test_longname").isin(["S -CA 15-3 Antigen", "S -CA 12-5 Antigen", "S -CA15-3 Antigen, comparison method", "S -Carcinoembryonic Antigen, comparison method", 
"S -CA 19-9 Antigen"]))
              .withColumn("date", F.to_date("sampling_time"))
              .selectExpr("social_security_number", "date", "test_longname as event", "result_id as id", "result as other")
              .distinct()
              )

#Metastasis texts
#Diagnosis...
time_diagnosis_text = (df_dict["texts"]
                         .withColumn("metastasis_diagnosis_codes", F.concat_ws(";", F.array_distinct("metastasis_diagnosis_codes")))
                         .withColumn("metastasis_sentences", F.concat_ws(";", F.array_distinct("metastasis_sentences")))
                         .filter((F.col("metastasis_diagnosis_codes").isNotNull()) & (F.col("metastasis_diagnosis_codes") != ""))
                         .withColumn("row_num", F.row_number().over(wind_dg_statement))
                         .filter((F.col("row_num")==F.lit(1)))
                         .withColumn("date", F.to_date("event_time"))
                         .withColumn("id", F.lit(None))
                         .selectExpr("social_security_number", "date", "metastasis_diagnosis_codes as event", "id", "metastasis_sentences as other")
                         .distinct()
                         )

#... and other notes
time_metastasis_text = (df_dict["texts"]
                         .withColumn("metastasis_sentences", F.concat_ws(";", F.array_distinct("metastasis_sentences")))
                         .filter((F.col("metastasis_sentences").isNotNull()) & (F.col("metastasis_sentences") != ""))
                         .withColumn("date", F.to_date("event_time"))
                         .withColumn("id", F.lit(None))
                         .withColumn("event", F.lit("Metastasis statement"))
                         .selectExpr("social_security_number", "date", "event", "id", "metastasis_sentences as other")
                         .distinct()
                         )

#Combining all the events
timeline_combined = (time_pat
                          .union(time_bc_op)
                          .union(time_medication_administration)
                          .union(time_medication_min)
                          .union(time_medication_max)
                          .union(time_diagnosis)
                          .union(time_diagnosis_text)
                          .union(time_lab)
                          .union(time_metastasis_text)
                          .join(df_dict["customer"].select("social_security_number", "epatient_id").distinct(), "social_security_number")
                          .sort("social_security_number", F.col("date").asc())
                          )
```

The outcome looks like this (with some made-up data):


```markdown
| SSN     | Date       | Event                   | ID        | Other                                            |
|---------|------------|-------------------------|-----------|--------------------------------------------------|
| ...     | ...        | ...                     | ...       | ...                                              |
| 83IL8IUS| 2018-07-09 | C50.40&                 |           | Upper quadrant breast cancer without histological confirmation |
| 83IL8IUS| 2018-07-09 | Metastasis statement    |           | Metastasis in the sentinel lymph node.           |
| 83IL8IUS| 2018-07-29 | Pathology               | JS36-8456 | Carcinoma ductale                                |
| 83IL8IUS| 2018-07-29 | Breast cancer operation |           |                                                  |
| 83IL8IUS| 2018-08-28 | S -CA 15-3 Antigen      | 90        | 20                                               |
| 83IL8IUS| 2018-09-09 | C79.8&                 |           | Metastasis in another specified location         |
| 83IL8IUS| 2018-09-09 | S -CA 15-3 Antigen      | 380       | 20                                               |
| 83IL8IUS| 2018-09-29 | Start Trastuzumab       |           | L01FD01                                          |
| 83IL8IUS| 2018-09-29 | Trastuzumab             |           | L01FD01                                          |
| 83IL8IUS| 2018-10-15 | Trastuzumab             |           | L01FD01                                          |
| ...     | ...        | ...                     | ...       | ...                                              |
| 83IL8IUS| 2019-05-20 | End Trastuzumab         |           | L01FD01                                          |
| 83IL8IUS| 2019-05-20 | Start Trastuzumab emtansine |         | L01FD03                                          |
| ...     | ...        | ...                     | ...       | ...                                              |

```

This table includes 10 empty rows where you can fill in the data as needed.