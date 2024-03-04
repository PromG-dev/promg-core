import os
from string import Template
from typing import Optional

from ..data_managers.semantic_header import SemanticHeader
from ..database_managers.db_connection import DatabaseConnection
from ..data_managers.datastructures import DatasetDescriptions
from ..utilities.performance_handling import Performance
from ..cypher_queries.data_importer_ql import DataImporterQueryLibrary as di_ql
from pathlib import Path
import pandas as pd


class Importer:
    def __init__(self, database_connection: DatabaseConnection,
                 data_structures: DatasetDescriptions,
                 semantic_header: SemanticHeader,
                 import_directory: Optional[str] = None,
                 use_sample: bool = False,
                 use_preprocessed_files: bool = False,
                 store_files: bool = False):
        self.connection = database_connection
        self.structures = data_structures.structures
        self.records = semantic_header.records

        self.load_batch_size = 20000
        self.use_sample = use_sample
        self.use_preprocessed_files = use_preprocessed_files
        self.store_files = store_files
        self.load_status = 0

        self._import_directory = import_directory

    def update_load_status(self):
        self.connection.exec_query(di_ql.get_update_load_status_query,
                                   **{
                                       "current_load_status": self.load_status
                                   })
        self.load_status += 1

    def import_data(self, to_be_imported_logs) -> None:
        for structure in self.structures:
            required_labels_str = structure.get_required_labels_str(records=self.records)

            # read in all file names that match this structure
            for file_name in structure.file_names:
                # only load files that are not imported yet
                if to_be_imported_logs is None or file_name in to_be_imported_logs:
                    # read and import the nodes
                    df_log = structure.read_data_set(file_name=file_name,
                                                     use_sample=self.use_sample,
                                                     use_preprocessed_file=self.use_preprocessed_files,
                                                     load_status=self.load_status,
                                                     store_preprocessed_file=self.store_files)

                    df_log = structure.determine_optional_labels_in_log(df_log, records=self.records)

                    self._import_nodes_from_data(df_log=df_log, file_name=file_name,
                                                 required_labels_str=required_labels_str)

                    if structure.has_datetime_attribute():
                        # once all events are imported, we convert the string timestamp to the timestamp as used in
                        # Cypher
                        self._reformat_timestamps(structure=structure, required_labels_str=required_labels_str)

                    # TODO: move filtering to pandas dataframe
                    self._filter_nodes(structure=structure,
                                       required_labels_str=required_labels_str)  # filter nodes according to the
                    # structure
                    self._finalize_import(required_labels_str=required_labels_str)  # removes temporary properties

    @Performance.track("structure")
    def _reformat_timestamps(self, structure, required_labels_str):
        datetime_formats = structure.get_datetime_formats()
        for attribute, datetime_format in datetime_formats.items():
            if datetime_format.is_epoch:
                self.connection.exec_query(di_ql.get_convert_epoch_to_timestamp_query,
                                           **{
                                               "required_labels_str": required_labels_str,
                                               "attribute": attribute,
                                               "datetime_object": datetime_format
                                           })

            self.connection.exec_query(di_ql.get_make_timestamp_date_query,
                                       **{
                                           "required_labels_str": required_labels_str,
                                           "attribute": attribute,
                                           "datetime_object": datetime_format,
                                           "load_status": self.load_status
                                       })

    @Performance.track("structure")
    def _filter_nodes(self, structure, required_labels_str):
        for exclude in [True, False]:
            attribute_values_pairs_filtered = structure.get_attribute_value_pairs_filtered(exclude=exclude)
            for name, values in attribute_values_pairs_filtered.items():
                self.connection.exec_query(di_ql.get_filter_records_by_property_query,
                                           **{
                                               "prop": name,
                                               "values": values,
                                               "exclude": exclude,
                                               "load_status": self.load_status,
                                               "required_labels_str": required_labels_str
                                           })

    @Performance.track("structure")
    def _finalize_import(self, required_labels_str):
        # finalize the import
        self.connection.exec_query(di_ql.get_finalize_import_records_query,
                                   **{
                                       "load_status": self.load_status,
                                       "required_labels_str": required_labels_str
                                   })
        self.load_status = 0

    @Performance.track("file_name")
    def _import_nodes_from_data(self, df_log, file_name, required_labels_str):
        grouped_by_optional_labels = df_log.groupby(by="labels")
        mapping_str = self._determine_column_mapping_str(df_log)

        for optional_labels_str, log in grouped_by_optional_labels:
            labels_str = required_labels_str + optional_labels_str
            new_file_name = self.determine_new_file_name(file_name, optional_labels_str)
            self.import_log_into_db(file_name=new_file_name, labels_str=labels_str, mapping_str=mapping_str, log=log)

    def import_log_into_db(self, file_name, labels_str, mapping_str, log):
        # Temporary save the file in the import directory
        self._save_log_grouped_by_labels(log=log, file_name=file_name)
        self.connection.exec_query(di_ql.get_create_nodes_by_loading_csv_query,
                                   **{
                                       "file_name": file_name,
                                       "labels": labels_str,
                                       "mapping": mapping_str
                                   })

        # delete the file from the import directory
        self._delete_log_grouped_by_labels(file_name=file_name)

    @staticmethod
    def determine_new_file_name(file_name, optional_labels_str):
        if optional_labels_str == "":
            return file_name
        return file_name[:-4] + "_" + optional_labels_str.replace(":", "_") + ".csv"

    def _save_log_grouped_by_labels(self, log, file_name):
        log = log.drop(columns=["labels"])
        log.to_csv(Path(self.get_import_directory(), file_name), index=False)

    def _delete_log_grouped_by_labels(self, file_name):
        path = Path(self.get_import_directory(), file_name)
        if os.path.exists(path):
            os.remove(path)

    @staticmethod
    def _determine_column_mapping_str(log):
        mapping = {}
        dtypes = log.dtypes.to_dict()
        for col_name, type in dtypes.items():
            if type == object:
                continue  # default is STRING
            elif pd.api.types.is_integer_dtype(type):
                mapping[col_name] = 'INTEGER'
            elif pd.api.types.is_float_dtype(type):
                mapping[col_name] = 'FLOAT'
            elif pd.api.types.is_bool_dtype(type):
                mapping[col_name] = 'BOOLEAN'
            else:
                raise Exception(f"Type for column {col_name} is not defined")

        template_str = '$col_name:{type:"$type"}'
        mapping_list = [Template(template_str).substitute({"col_name": col_name, "type": type}) for col_name, type in
                        mapping.items()]
        mapping_str = '{' + ','.join(mapping_list) + '}'
        return mapping_str

    def retrieve_import_directory(self):
        result = self.connection.exec_query(di_ql.get_import_directory_query)
        # get the correct value from the result
        self._import_directory = result[0]['directory']

    def get_import_directory(self):
        if self._import_directory is None:
            self.retrieve_import_directory()
        return self._import_directory
