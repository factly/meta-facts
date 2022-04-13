# meta-facts

Automatic generation of Meta-Data for a dataset

------

<details open>
<summary id = "table-of-content">Table of Contents</summary>

1. [Motivation](#motivation) 
2. [How to run the application](#how-to-run-the-application)
3. [Project Structure](#project-structure)
4. [Methodology](#methodology)
   1. Where this Library fits in the overall architecture
   2. Approach to determine Meta-Data
      1. File Path
      2. [Units](#units)
      3. [Temporal Coverage](#temporal-coverage)
      4. [Granularity](#granularity)  
      5. [Spatial Coverage](#spatial-coverage)
      6. [File Formats Available](#file-formats-available)
      7. Is Public Dataset

</details>

---

<h2 id = "motivation">Motivation</h3>

----


<h2 id = "how-to-run-the-application">How to run the application</h3>


#### Runnning Localhost

```bash
poetry run uvicorn app.main:app --reload --port 8005
```

#### Deploy app

```bash
docker compose up --build
```

#### Access Swagger Documentation

> <http://localhost:8005/api/docs>


----

<h2 id = "project-structure">Project structure</h2>

Files related to application are in the `app` or `tests` directories.
Application parts are:

    app
    ├── api              - web related stuff.
    │   └── routes       - web routes.
    ├── core             - application configuration, startup events, logging.
    ├── models           - pydantic models for this application.
    ├── services         - logic that is not just crud related.
    └── main.py          - FastAPI application creation and configuration.
    │
    tests                  - pytest

----

<h2 id = "methodology">Methodology</h2>
<br>
<h3>Approach to determine Meta-Data</h3>
<br>
<h4 id = "units"><b>Units :</b></h4>

- *General Workflow*

  ```mermaid
  graph LR;
    A[Dataset]-->B{Unit Column Exists ?};
    
    B -- NO --> C(RETURN Null String);
    B -- Yes --> D[Get all  unique units from UNIT Column];

    D --> E[Prepare List of all separate units];
    E --> F(RETURN all units as STRING SEPARATED WITH COMMAS)
  ```


<div align="right"><a = href= "#table-of-content">Table of Content</a></div>

---

<h4 id = "temporal-coverage"><b>Temporal Coverage :</b></h4>

- *General Workflow*

  ```mermaid
  flowchart LR

  A(Dataset) -->  B{Year column exists ?}
  B -- NO --> C(RETURN Null String) 
  B -- Yes --> D[Calender / Non-Calender Year Columns]
  D --> E{Years are in Sequence ?}
  E -- YES --> F(RETURN string represntation of range \n example : 2012 to 2020 or \n 2012-13 to 2020-21)
  E -- NO --> G(RETURN  comma separated values for all years, \n exmaple : 2012,2015,2018 or \n 2012-13, 2015-16, 2018-19)
  ```

  *Notes:*
  - Determination of Temporal coverage is based on the presence of year column.
  - If  both Calender year and Non-Calender year are presnet in dataset then priority will be given to Calender year.

<div align="right"><a = href= "#table-of-content">Table of Content</a></div>

---

<h4 id = "granularity"><b>Granulaity :</b></h4>

- *General Workflow*

  ```mermaid
    flowchart LR
    A(Dataset) --> B{If any of Date-time or \nGeography columns exists ?}
    B -- No --> C(RETURN Null String)
    B -- YES -->  D[Map all Columns levels in \nSorted Order for respective Domains]
    D --> E[Map the columns groups according to \nproper naming convention Granularity]
    E --> F(RETURN Comma Separated Values of all Granularitues \n example : Quarterly, District)
  ```


  *Notes:*
  - Granularity is calculated for 2 domains.
    - Geography
    - Date-Time
  - In `config.py` There are granularity ranks mentioned for each domain.
  - In `config.py` there are Keywords also present for Granularity if found in Datasets.

<div align="right"><a = href= "#table-of-content">Table of Content</a></div>

---

<h4 id = "spatial-coverage"><b>Spatial Coverage :</b></h4>

Mentioned below are the Cases for Spatial Covererage : 
  | Spatial Location             | Dataset with categories as   | Methodology                                 | Spatial Coverage                                     |
  | ---------------------------- | ---------------------------- | ------------------------------------------- | ---------------------------------------------------- |
  | Countries                    | India, Pakisthan, China, etc |                                             | **Country**                                          |
  | Specific Country             | India                        | represent it with the specific Country Name | **India**                                            |
  | States of a Country          | Andhra Pradesh, Assam, etc   |                                             | **States of India**                                  |
  | Regions of a country         | South India, NE states etc   |                                             | **Regions of India**                                 |
  | Specific State of a country  | Andhra Pradesh               | represent it with the specific State Name   | **Andhra Pradesh**                                   |
  | Districts of a State/ States | Adilabad, Hyderabad etc      |                                             | **Districts of Telangana** or **Districts of India** |
  | Specific District of a state | Hyderabad                    | represent it with specific District Name    | **Hyderabad**                                        |

<br>
  
- *General Workflow*


  ```mermaid
    flowchart LR
    A(Dataset) --> B{If Geographical Columns exists ?}
    B -- NO --> C(RETURN Default Value as INDIA)
    B -- YES --> D[Sort the order of different \nGeographical Level]
    D --> E(RETURN Value of biggest order of Geographical Column \nwith proper naming convention)
  ```
  


  *Notes:*
  - This library currently facilitates only for Country, State and District level of Spatial Coverage.
  - Mapping of levels of Geographic Columns is decided by corresponding column names and not the values, hence change in Column names will impact the mapping.
  - If there is no Geographic column , then the result would be default for INDIA.
  - Spatial coverage order, keyword Mapping and Naming Convention are mentioned in `config.py`.

<div align="right"><a = href= "#table-of-content">Table of Content</a></div>

---

<h4 id = "file-formats-available"><b>File Formats Available :</b></h4>


  *Notes:*
  - Reads the format of file from the file name.

<div align="right"><a = href= "#table-of-content">Table of Content</a></div>