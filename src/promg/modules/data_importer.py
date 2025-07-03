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


def pop_log_name(log):
    # get log name if it is in the log
    if "log" in log.columns:
        log_names = log.pop("log")
        log_names = log_names.unique()
        if len(log_names) > 1:
            # todo make error
            raise Exception("Each file should originate from exactly one log, now there are more logs defined.")
        else:
            log_name = log_names[0]
    else:
        log_name = None
    return log, log_name


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

        self._import_directory = import_directory

    def import_data(self, to_be_imported_logs) -> None:
        for structure in self.structures:
            required_labels = structure.get_required_labels(records=self.records)

            # read in all file names that match this structure
            for file_name in structure.file_names:
                # only load files that are not imported yet
                if to_be_imported_logs is None or file_name in to_be_imported_logs:
                    # read and import the nodes
                    df_log = structure.read_data_set(file_name=file_name,
                                                     use_sample=self.use_sample,
                                                     use_preprocessed_file=self.use_preprocessed_files,
                                                     store_preprocessed_file=self.store_files)

                    df_log = structure.determine_optional_labels_in_log(df_log, records=self.records)

                    self._import_nodes_from_data(df_log=df_log,
                                                 file_name=file_name,
                                                 required_labels=required_labels)

                    if structure.has_datetime_attribute():
                        # once all events are imported, we convert the string timestamp to the timestamp as used in
                        # Cypher
                        self._reformat_timestamps(structure=structure, required_labels=required_labels)

                    # TODO: move filtering to pandas dataframe
                    self._filter_nodes(structure=structure,
                                       required_labels=required_labels)  # filter nodes according to the
                    # structure

        # self._clear_import_directory()

    @Performance.track("structure")
    def _reformat_timestamps(self, structure, required_labels):
        datetime_formats = structure.get_datetime_formats()
        for attribute, datetime_format in datetime_formats.items():
            if datetime_format.is_epoch:
                self.connection.exec_query(di_ql.get_convert_epoch_to_timestamp_query,
                                           **{
                                               "required_labels": required_labels,
                                               "attribute": attribute,
                                               "datetime_object": datetime_format
                                           })

            self.connection.exec_query(di_ql.get_make_timestamp_date_query,
                                       **{
                                           "required_labels": required_labels,
                                           "attribute": attribute,
                                           "datetime_object": datetime_format
                                       })

    @Performance.track("structure")
    def _filter_nodes(self, structure, required_labels):
        for exclude in [True, False]:
            attribute_values_pairs_filtered = structure.get_attribute_value_pairs_filtered(exclude=exclude)
            for name, values in attribute_values_pairs_filtered.items():
                self.connection.exec_query(di_ql.get_filter_records_by_property_query,
                                           **{
                                               "prop": name,
                                               "values": values,
                                               "exclude": exclude,
                                               "required_labels": required_labels
                                           })

    @Performance.track("file_name")
    def _import_nodes_from_data(self, df_log, file_name, required_labels):
        grouped_by_optional_labels = df_log.groupby(by="labels")
        mapping_str = self._determine_column_mapping_str(df_log)

        for optional_labels_str, log in grouped_by_optional_labels:
            optional_labels = optional_labels_str.split(":")
            labels = required_labels + optional_labels
            labels = list(set(labels))
            labels.remove("")
            new_file_name = self.determine_new_file_name(file_name, optional_labels_str)
            self.import_log_into_db(file_name=new_file_name, labels=labels, mapping_str=mapping_str, log=log)

    def import_log_into_db(self, file_name, labels, mapping_str, log):
        # Temporary save the file in the import directory
        log, log_name = pop_log_name(log)

        self._save_log_grouped_by_labels(log=log, file_name=file_name)
        # first create the record types and log nodes
        self.connection.exec_query(di_ql.get_create_record_types_and_log_query,
                                   **{
                                       "labels": labels,
                                       "log_name": log_name
                                   })
        # when creating the records nodes, relations between the record types and log nodes are created
        self.connection.exec_query(di_ql.get_create_nodes_by_loading_csv_query,
                                   **{
                                       "file_name": file_name,
                                       "log_name": log_name,
                                       "labels": labels,
                                       "mapping": mapping_str
                                   })

    @staticmethod
    def determine_new_file_name(file_name, optional_labels_str):
        if optional_labels_str == "":
            return file_name
        return file_name[:-4] + "_" + optional_labels_str.replace(":", "_") + ".csv"

    def _save_log_grouped_by_labels(self, log, file_name):
        log = log.drop(columns=["labels"])
        log.to_csv(Path(self.get_import_directory(), file_name), index=False)


    def _clear_import_directory(self):
        folder = Path(self.get_import_directory())
        if os.path.exists(folder):
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                except Exception as e:
                    print('Failed to delete %s. Reason: %s' % (file_path, e))

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
