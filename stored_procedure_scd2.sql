--The scd2_emp stored procedure implements a Slowly Changing Dimension (SCD) Type 2 pattern, 
--which is commonly used in data warehousing. This procedure helps manage historical data for employee records, allowing for tracking of changes over time.


--Stored procedure:

CREATE OR REPLACE PROCEDURE scd2_emp() AS
$$
BEGIN  --A temporary table named temp_gl is created to store intermediate results.
--This table includes various columns for employee attributes and flags to manage insertions and updates.
    CREATE TEMPORARY TABLE temp_gl (
        employee_id int,
        first_name varchar(50),
        last_name varchar(50),
        email varchar(50),
        phone_number varchar(50),
        hire_date date,
        job_id varchar(50),
        salary int,
        commission_pct varchar(50),
        manager_id int,
        department_id int,
        start_date date,
        end_date varchar(50),
        hash varchar(100),
        flag varchar(10),
        upsert_flag varchar(10)
    );
--Data is inserted into temp_gl from the employee_stg staging table. 
--It retrieves distinct employee records, using a left join to compare against existing records in employee_main.
    INSERT INTO temp_gl (
        employee_id ,
        first_name ,
        last_name ,
        email ,
        phone_number ,
        hire_date ,
        job_id ,
        salary ,
        commission_pct ,
        manager_id ,
        department_id ,
        start_date ,
        end_date ,
        hash ,
        flag,
        upsert_flag
    )
--The upsert_flag column is determined using a CASE statement:
--Insert: If an employee exists in the staging table but not in the main table.
--Update: If an employee exists in both tables but has different hash values (indicating changes).
    SELECT
        distinct(stg.employee_id),
        stg.first_name ,
        stg.last_name ,
        stg.email ,
        stg.phone_number ,
        stg.hire_date ,
        stg.job_id ,
        stg.salary ,
        stg.commission_pct ,
        stg.manager_id ,
        stg.department_id ,
        stg.start_date ,
        stg.end_date ,
        stg.hash ,
        stg.flag,
        CASE
            WHEN stg.employee_id IS NOT NULL AND main.employee_id IS NULL THEN 'insert'
            WHEN stg.employee_id = main.employee_id AND stg.hash != main.hash THEN 'update'
            ELSE NULL
        END AS upsert_flag
    FROM employee_stg stg
    LEFT JOIN employee_main main ON stg.employee_id = main.employee_id;
--The procedure then inserts new and updated records into the employee_main table from the temporary table. 
--It ensures only records marked for insertion or update are processed.
	INSERT INTO employee_main(
        employee_id ,
        first_name ,
        last_name ,
        email ,
        phone_number ,
        hire_date ,
        job_id ,
        salary ,
        commission_pct ,
        manager_id ,
        department_id ,
        start_date ,
        end_date ,
        hash ,
        flag
    )
--Existing records in employee_main are updated to mark them as inactive (with flag = '0') and set their end_date to the current date 
--when their hash values differ from those in the staging data. This effectively closes out old versions of records.
    (SELECT stg.employee_id ,
        stg.first_name ,
        stg.last_name ,
        stg.email ,
        stg.phone_number ,
        stg.hire_date ,
        stg.job_id ,
        stg.salary ,
        stg.commission_pct ,
        stg.manager_id ,
        stg.department_id ,
        stg.start_date ,
        stg.end_date ,
        stg.hash ,
        stg.flag
    FROM employee_stg stg
    JOIN temp_gl t ON stg.employee_id = t.employee_id
    WHERE t.upsert_flag = 'insert' OR t.upsert_flag = 'update');
    
    UPDATE employee_main AS em
    SET flag = '0', end_date = current_date
    FROM temp_gl AS tg
    WHERE em.employee_id = tg.employee_id
        AND em.hash != tg.hash
        AND tg.upsert_flag = 'update';
        
    DROP TABLE  temp_gl;
	--The temporary table is dropped after use, and the employee_stg table is truncated to prepare for the next load cycle.
    truncate table employee_stg;
END;
$$
LANGUAGE plpgsql;


call scd2_emp();


select * from employee_main;
