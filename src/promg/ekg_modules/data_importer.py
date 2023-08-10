import math
from typing import List

import numpy as np
from tqdm import tqdm

from ..data_managers.semantic_header import RecordConstructor
from ..database_managers.db_connection import DatabaseConnection
from ..data_managers.datastructures import ImportedDataStructures
from ..utilities.performance_handling import Performance
from ..cypher_queries.data_importer_ql import DataImporterQueryLibrary as di_ql
import pandas as pd


class Importer:
    def __init__(self, db_connection: DatabaseConnection, data_structures: ImportedDataStructures,
                 records: List["RecordConstructor"], batch_size: int,
                 use_sample: bool = False, use_preprocessed_files: bool = False):
        self.connection = db_connection
        self.structures = data_structures.structures
        self.records = records

        self.batch_size = batch_size
        self.use_sample = use_sample
        self.use_preprocessed_files = use_preprocessed_files

    def import_data(self) -> None:
        for structure in self.structures:
            labels = structure.labels
            file_directory = structure.file_directory
            # read in all file names that match this structure
            for file_name in structure.file_names:
                # read and import the nodes
                df_log = structure.read_data_set(file_directory, file_name, use_sample=self.use_sample,
                                                 use_preprocessed_file=self.use_preprocessed_files)
                df_log["loadStatus"] = 0
                self._import_nodes_from_data(labels=labels, df_log=df_log, file_name=file_name)

            if structure.has_datetime_attribute():
                # once all events are imported, we convert the string timestamp to the timestamp as used in Cypher
                self._reformat_timestamps(structure=structure)

            self._filter_nodes(structure=structure)  # filter nodes according to the structure

            self._finalize_import(structure=structure)  # removes temporary properties

    @Performance.track("structure")
    def _reformat_timestamps(self, structure):
        datetime_formats = structure.get_datetime_formats()
        for attribute, datetime_format in datetime_formats.items():
            if datetime_format.is_epoch:
                self.connection.exec_query(di_ql.get_convert_epoch_to_timestamp_query,
                                           **{
                                               "attribute": attribute,
                                               "datetime_object": datetime_format,
                                               "batch_size": self.batch_size
                                           })

            self.connection.exec_query(di_ql.get_make_timestamp_date_query,
                                       **{
                                           "attribute": attribute, "datetime_object": datetime_format,
                                           "batch_size": self.batch_size
                                       })

    @Performance.track("structure")
    def _filter_nodes(self, structure):
        for boolean in (True, False):
            attribute_values_pairs_filtered = structure.get_attribute_value_pairs_filtered(exclude=boolean)
            for name, values in attribute_values_pairs_filtered.items():
                self.connection.exec_query(di_ql.get_filter_events_by_property_query,
                                           **{"prop": name, "values": values, "exclude": boolean})

    @Performance.track("structure")
    def _finalize_import(self, structure):
        # finalize the import
        self.connection.exec_query(di_ql.get_finalize_import_events_query,
                                   **{
                                       "batch_size": self.batch_size
                                   })

    @Performance.track("file_name")
    def _import_nodes_from_data(self, labels, df_log, file_name):
        # start with batch 0 and increment until everything is imported
        batch = 0
        print("\n")
        pbar = tqdm(total=math.ceil(len(df_log) / self.batch_size), position=0)
        record_constructors = self._get_record_constructors_by_labels(labels)
        while batch * self.batch_size < len(df_log):
            pbar.set_description(f"Loading data from {file_name} from batch {batch}")

            # import the events in batches, use the records of the log
            batch_without_nans = [{k: int(v) if isinstance(v, np.integer) else v for k, v in m.items()
                                   if (isinstance(v, list) and len(v) > 0) or (not pd.isna(v) and v is not None)}
                                  for m in
                                  df_log[batch * self.batch_size:(batch + 1) * self.batch_size].to_dict(
                                      orient='records')]
            self.connection.exec_query(di_ql.get_create_nodes_by_importing_batch_query,
                                       **{"batch": batch_without_nans,
                                          "record_constructors": record_constructors})

            pbar.update(1)
            batch += 1
        #     if (batch % 100) == 0:
        #         self._create_records(labels)
        #
        # self._create_records(labels)
        pbar.close()

    def _get_record_constructors_by_labels(self, labels):
        constructors = []
        for record_constructor in self.records:
            if labels is not None:
                intersection = list(set(labels) & set(record_constructor.record_labels))
            else:
                intersection = record_constructor.record_labels
            if len(intersection) > 0: # label of record constructor is in intersection or labels is not defined
                constructors.append(record_constructor)
        return constructors

    def _create_records(self, labels) -> None:
        record_constructors = self._get_record_constructors_by_labels(labels)
        self.connection.exec_query(di_ql.get_create_record_query,
                                   **{
                                       "record_constructors": record_constructors,
                                       "batch_size": self.batch_size
                                   })
        self.connection.exec_query(di_ql.get_reset_added_label_query,
                                   **{
                                       "batch_size": self.batch_size
                                   })
        self.connection.exec_query(di_ql.get_update_load_status_query,
                                   **{
                                       "current_load_status": 0
                                   })
