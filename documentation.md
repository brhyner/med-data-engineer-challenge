# Documentation Medgate Challenge

The setup that is described below leverages ``dbt``, ``postgres`` and ``airflow`` to complete the challenge. The three tools are all spin up inside a docker setup.

## Step 1
Create virtual environment:
```
python -m venv ve_challenge
```
activate the environment:

```
ve_challenge/Scripts/activate
>> pip freeze
```

## Step 2
create ``requirements.txt`` file.

Install dependencies:
``
pip install -r requirements.txt
``

## Step 3 

Create dbt project 
``
dbt init
``

follow setup process. A new directory is created. Also, copy over the profiles information from the default .dbt/profiles.yml and move it to the same dbt directory so docker can better access it and so that the information from other dbt projects is not exposed in the docker container.


## Step 4: Work with the data
My idea is to preprocess the data inside the ``/data`` folder to have ``.csv`` files that I can then use in dbt by using ``dbt seed``. This approach means that I write a python script that does some transformations from ``.ndjson`` to ``.csv`` and then move the files from the original directory into the ``seeds`` folder within the dbt project directory. 

Other possibilities to handle this would be to directly write python scripts that handle the transformation and loading of the data directly to the postgres db. Of course, within these different approaches there are a lot of other different approaches nested. I wanted to primarily work with dbt and not rely too much on python. However, in terms of performance this might be suboptimal, since ``dbt seed`` takes a while with the files in this project.

For preprocessing, I need to perform three actions (if I want to run everything within a single dag in Airflow):
- Make sure all the dependencies are installed --> see ``scripts/install_dependencies.sh``
- transform data to be in ``.csv`` format --> see ``preprocess_data()`` inside ``dags/run_dbt_workflow.py``
- move data to the ``seeds`` directory inside the dbt directory --> see ``scripts/move_csv.sh``

Running these would result in the data landing in the ``medgate_challenge/seeds`` directory. In ``medgate_challenge`` I then can run ``dbt seed`` which loads data to the defined schema in the postgres db. In order to make sure that the datatypes are correct, I created the ``src_seeds.yml``. I also rely on a ``dbt utils`` macro to create a surrogate key, hence I added a ``packages.yml`` file.

When everything is set up, I start to write the dbt models so that analysts can use the data to create their own queries and analyze the data. In the end I have 4 staging models and 4 final models. I decided to separate the cases from the icpc codes, because otherwise the cases table would not have a unique case id. For the association table of cases with icpc cases I created a surrogate key. I also created a ``case type`` dimension table. Where it made sense, I configured the dbt models to be incremental with a ``merge`` strategy.

Finally, in order to answer the following questions, I created models in the ``analyses`` directory:
- Number of cases per patient
- average age
- most common icpc codes

### Analyzing data

#### Number of cases per patient

```
with patients as (

    select 
        patient_id
    from {{ ref('mas_dim__patients') }}
)

, cases as (

    select 
        case_id
        , patient_id
        , case_closed

    from {{ ref('mas_fct__cases') }}
    where 
        case_closed is not null 

)

, joined as (

    select 
        p.patient_id
        , c.case_id
        , c.case_closed
    from patients as p 
    left join cases as c 
    on 
        p.patient_id = c.patient_id
)

, aggregation as (

    select 
        patient_id 
        , count(
            distinct 
            CASE 
                WHEN not case_closed THEN case_id
            END
        ) as open_cases
        , count(
            distinct 
            CASE 
                WHEN case_closed THEN case_id
            END
        ) as closed_cases
        , count(distinct case_id) as total_cases
    from joined 

    group by 
        1
)

select 
    *
    -- not really clean, but depends what exactly the goal is
        -- number of cases per patient as a rollup or overall number of cases per patient (i.e. average cases per patient)
        -- from an analytical point of view, the average is more useful. 
        -- operationally, the per patient numbers are more useful.
    , AVG(total_cases) OVER () as average_cases_per_patient_overall
from aggregation 
```

From an analytical perspective, it would make more sense to have the average number of cases per patient (which is around 6). However, operationally, the per patient number of cases might be more valuable. 

#### Average age

```
with base as (

    select 
        DATE_PART('year', AGE(patient_date_of_birth)) as patient_age
    from {{ ref('mas_dim__patients') }}
    where 
        patient_date_of_birth is not null 
        -- make sure that there are no negative ages
        AND patient_date_of_birth < CURRENT_DATE
)

select 
    AVG(patient_age) as average_age
from base

-- average age
    -- 59
```

When looking through the data, I noticed that some patients were lacking a data for their birthdate and some had one in the future. Hence, I added some filter conditions. Additionally, I noticed quite old people in the dataset which might not be correct (some were older than 100). Disregarding this fact, I end up with an average age of about 59.

#### Most common icpc codes

```
{% set topn = 10 %}

with base as (

    select 
        icpc_code
        , count(distinct icpc_code_id) as number_of_cases
    from {{ ref('mas_fct__cases-with-icpc-codes') }}

    group by 
        1
)

, result as (

    select 
        *
    from base
    order by 
        2 DESC
    limit {{ topn }}
)

select 
    * 
from result
```

The query above results in the top 10 most common icpc codes in the sample. 


## Step 5
Build the Airflow DAG:

I create the DAG in the ``dags/medgate_pipeline.py`` file. In there, i first copy paste the code from the preprocess data part from earlier that transforms the data to csv. In doing so, I then can call the definition inside the DAG using a python operator.

In total, the DAG consists of 4 Tasks:
- Installing the dependencies
- Transforming the data to csv files
- moving the csv files to the dbt directory
- executing dbt deps & dbt build (which includes dbt seeds) 


## Step 6
Containerize the whole thing! 

For this, I use ``docker-compose.yml`` file that contains multiple services/containers:
- postgres
- redis
- airflow-init
- airflow-scheduler
- airflow-worker
- airflow-triggerer
- airflow-webserver
- dbt

Now technically, I could have also not added dbt as a separate service and just built a custom airflow image that included a dbt installation. However, this setup also works.

The definitions for the airflow services comes directly from a template provided by airflow.

## Step 7
Compose the setup and run the pipeline.

In order to run this setup, you first have to compose the containers:
```
docker-compose up -d --build
```

This creates and starts the containers. When going to ``localhost:8080`` you will be prompted to login to Airflow (login details can be found in ``docker-compose.yml`` under the environment of ``airflow-init``). Inside Airflow, you can trigger the pipeline manually where it takes around 3mins to complete. Notice that this pipeline can easily be scheduled by changing the ``scheduling_interval`` argument inside the ``DAG()`` run:

```
# in /dags/medgate_pipeline.py

# Define the DAG
with DAG(
    'medgate_pipeline',
    default_args=default_args,
    schedule_interval='@daily', # run once a day at midnight
    start_date=datetime(2024, 1, 1),
) as dag:

```

If you are finished then do 

```
docker-compose down
```

to delete the containers. Note that the data from postgres will be persisted, because there is a named volume defined in ``docker-compose.yml``.

Have fun testing!