
# Suvera Data Take-home Answers - David Pollock

dbt Setup:
* I completed this assigment using dbt Cloud with a Snowflake connection
* Project was duplicated from Git using "git clone --bare" and "git push --mirror" to my public repo

Questions / Tasks:
* Unfortunately the raw data has poor data quality. How can we handle data quality and integrity?
    * So firstly we want to look through each file and explore the data. It's easy to see from the raw_pcns file
    * that there isn't any cleaning that needs done. As for the other three files, we want to approach the cleaning
    * file by file.
    * raw_patients:
        * We can see the csv file is made up of numerous JSON schemas, so we'll need to parse those before we can work with 
        * the data. 
        * We can also see that eah of the JSON schemas contains an array of conditions, this means that we'll have a possible one-to-many
        * relationship between patient_id and condition. A good approach here is to split the raw_patient data into two different
        * models; one for patient_data and patient_conditions before we can answer the below questions. See patient_data.sql and patient_conditions.sql.
        * Once we've done this, we can begin clenaing the data for each new model; patient_data and patient_conditions.
            * patient_data:
                * Examining the distinct values for each column we can immediately see "null" strings instead of NUlL, "invalid" and some nonsensical 
                * numerical values.
                * Applying appropriate CASE statements and REGEXP_REPLACE, we can clean the patient_data model
            * patient_conditions:
                * Same approach as for patient_data above however, more importantly we want to be sure that we flatten the JSON array for conditions
                * to create the on-to-many relationship for each patients_id and their conditions.
    * raw_pcns:
        * Although only 2 records exist in this file, we'll create some logic to remove duplicates incase more records are added in the future
    * raw_activities:
        * Apart from the removing of potentially duplicated records, there's only two items we want to clean here; correcting the column name of
        * activity_date to activity_date_time so it's more in line with the data it holds, and removing negative numbers from duration_minutes. We 
        * want to assume any negative number here is a typo (including the number), so we'll mark them as NULL.
    * raw_practices:
        * As for raw_pcns, we'll simply remove duplicates incase more records are added.

* How many patients belong to each PCN?
    * See patients_per_pcn.sql

* What's the average patient age per practice?
    * See avg_age_per_practice.sql

* Categorize patients into age groups (0-18, 19-35, 36-50, 51+) and show the count per group per PCN
    * See age_groups_per_pcn.sql

* What percentage of patients have Hypertension at each practice?
    * See hypertension_per_practice.sql

* For each patient, show their most recent activity date
    * For this question, it could be as simple as finding the MAX() activity date and grouping by patient_id however,
    * if we use a window function we can also see the activity_type, which is a little more useful as it would likely 
    * be a follow up question.
    * See patient_recent_activity.sql 

* Find Patients who had no activity for 3 months after their first activity
    * Given the fact that some patients could only have had 1 activity, we'll use anti join to answer the question to avoid issues with the
    * DATEDIFF() function.
    * See months_no_activity.sql
