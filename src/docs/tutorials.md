# Tutorial for BPIC'17 Dataset

## Download Datasets and place in correct directory
We use the BPIC'17 dataset, dataset descriptions and semantic header available from

`Swevels, Ava, & Fahland, Dirk. (2023). Event Data and Semantic Header for OCED-PG [Data set]. Zenodo. https://doi.org/10.5281/zenodo.8296559`

We assume the dataset to be present in `/data/BPIC17`. If you want to change the directory, you have to change the directory in the dataset description (`BPIC17_DS.json`) as well.

The semantic header (`BPIC17.json`) and the dataset description (`BPIC17_DS.json`) to be present in `[json_directory]`.

## Install PromG
You can install PromG using `pip install promg`.

## Set up variables
We first set some common variables used in the main script.

```py title="Set up parameters"
# import required packages
import os
from pathlib import Path

dataset_name = 'BPIC17' # name of the dataset
# use a sample when importing the data, useful for testing
use_sample = False 
 # the size for our batch queries, recommended from 10000 to 100000.
batch_size = 100000
# use preprocessed files, saves times (skip preprocessing)
# if these files don't exist, they will be created
use_preprocessed_files = False 

# set the paths for the semantic header and the dataset description
semantic_header_path = Path(f'[json_directory]/{dataset_name}.json')
ds_path = Path(f'json_files/{dataset_name}_DS.json')
```

## Determine settings
Depending on your analysis, you want to execute some steps. 
We control these steps using booleans.

```py title="Determine settings"
# import required packages
step_clear_db = True
step_populate_graph = True
step_discover_model = True
step_build_tasks = True
step_infer_delays = True
verbose = False
```

## Set Credentials
If you want to use the default credentials, you can use the following piece of code.

```python title="Set Default Credentials"
from promg import authentication

# set the credentials key to default
credentials_key = authentication.Connections.LOCAL
```

If you want to have custom credentials (i.e. remote), you can use the following piece of code.
```python title="Set Custom Credentials"
from promg.database_managers.authentication import Credentials
# if you want a remote/other connection, 
# store credentials somewhere different (e.g. remote_authentication)

# custom credentials
custom = Credentials(
    uri="your_uri", #e.g. uri="bolt://localhost:7687",
    user="your_user", #e.g. user="neo4j",
    password="your_password" #e.g. password="12345678"
)

# if you have created your own credentials
credentials_key = custom
```

## Create Semantic header and dataset description objects

```py title="Create Semantic header and dataset description objects"
from promg import SemanticHeader, DatasetDescriptions

# create semantic header object
semantic_header = SemanticHeader.create_semantic_header(semantic_header_path)

# create dataset description object
dataset_descriptions = DatasetDescriptions(ds_path)
```

## Set up database connection
```py title="Create DatabaseConnection"
from promg import DatabaseConnection
db_connection = DatabaseConnection.set_up_connection_using_key(key=credentials_key,
                                                                   verbose=verbose)
```

## Set up Performance Tracker
The performance tracker let's you know the progress of the steps and also outputs a performance log
Stored in `\perf\`.

```py title="Create Performance"
from promg import Performance
performance = Performance.set_up_performance(dataset_name=dataset_name,
                                                 use_sample=use_sample)
```

## Clear Database
Sometimes you want to clear the entire database, you can do so using the following piece of code.

```py title="Clear Database"
from promg.modules.db_management import DBManagement
db_manager = DBManagement()

if step_clear_db:
    print(Fore.RED + 'Clearing the database.' + Fore.RESET)
    db_manager.clear_db(replace=True)
    db_manager.set_constraints()
```

## Load and Transform the database using OcedPG

```py title="Populate Graph using OcedPG"
    from promg import OcedPg

    if step_populate_graph:
        if use_preprocessed_files:
            print(Fore.RED + '💾 Preloaded files are used!' + Fore.RESET)
        else:
            print(Fore.RED + '📝 Importing and creating files' + Fore.RESET)

        oced_pg = OcedPg(dataset_descriptions=dataset_descriptions,
                         use_sample=use_sample,
                         use_preprocessed_files=use_preprocessed_files)
        oced_pg.load_and_transform()
        oced_pg.create_df_edges()
```

## Identify Tasks

```py title="Identify Tasks"
    from promg.modules.task_identification import TaskIdentification

    if step_build_tasks:
        print(Fore.RED + 'Detecting tasks.' + Fore.RESET)
        task_identifier = TaskIdentification(resource="Resource", case="CaseAWO")
        task_identifier.identify_tasks()
        task_identifier.aggregate_on_task_variant()
```

## Custom modules
To be documented.

## Close database connection and finish performance
```python title="finish code"

    performance.finish_and_save()
    db_manager.print_statistics()

    db_connection.close_connection()
```

## Complete code
```python title="Complete Code"

import os
from datetime import datetime
from pathlib import Path

from promg import SemanticHeader, OcedPg
from promg import DatabaseConnection
from promg import authentication
from promg import DatasetDescriptions

from promg import Performance
from promg.modules.db_management import DBManagement
from promg.modules.task_identification import TaskIdentification

# several steps of import, each can be switch on/off
from colorama import Fore

dataset_name = 'BPIC17'
use_sample = False
batch_size = 10000
use_preprocessed_files = False

semantic_header_path = Path(f'json_files/{dataset_name}.json')

semantic_header = SemanticHeader.create_semantic_header(semantic_header_path)

ds_path = Path(f'json_files/{dataset_name}_DS.json')
dataset_descriptions = DatasetDescriptions(ds_path)

step_clear_db = False
step_populate_graph = False
step_delete_parallel_df = False
step_discover_model = True
step_build_tasks = False
step_infer_delays = True

verbose = False
credentials_key = authentication.Connections.LOCAL


def main() -> None:
    """
    Main function, read all the logs, clear and create the graph, perform checks
    @return: None
    """
    print("Started at =", datetime.now().strftime("%H:%M:%S"))

    db_connection = DatabaseConnection.set_up_connection_using_key(key=credentials_key,
                                                                   verbose=verbose)
    performance = Performance.set_up_performance(dataset_name=dataset_name,
                                                 use_sample=use_sample)
    db_manager = DBManagement()

    if step_clear_db:
        print(Fore.RED + 'Clearing the database.' + Fore.RESET)
        db_manager.clear_db(replace=True)
        db_manager.set_constraints()

    if step_populate_graph:
        if use_preprocessed_files:
            print(Fore.RED + '💾 Preloaded files are used!' + Fore.RESET)
        else:
            print(Fore.RED + '📝 Importing and creating files' + Fore.RESET)

        oced_pg = OcedPg(dataset_descriptions=dataset_descriptions,
                         use_sample=use_sample,
                         use_preprocessed_files=use_preprocessed_files)
        oced_pg.load_and_transform()
        oced_pg.create_df_edges()

    if dataset_name == 'BPIC17':
        if step_build_tasks:
            print(Fore.RED + 'Detecting tasks.' + Fore.RESET)
            task_identifier = TaskIdentification(resource="Resource", case="CaseAWO")
            task_identifier.identify_tasks()
            task_identifier.aggregate_on_task_variant()

    performance.finish_and_save()
    db_manager.print_statistics()

    db_connection.close_connection()


if __name__ == "__main__":
    main()



```