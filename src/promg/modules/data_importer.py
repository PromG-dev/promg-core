import math
from typing import List, Dict, Union

import numpy as np
from pandas import DataFrame
from tqdm import tqdm

from ..data_managers.semantic_header import RecordConstructor, SemanticHeader
from ..database_managers.db_connection import DatabaseConnection
from ..data_managers.datastructures import DatasetDescriptions, DataStructure
from ..utilities.performance_handling import Performance
from ..cypher_queries.data_importer_ql import DataImporterQueryLibrary as di_ql
import pandas as pd


class Importer:
    """
        Create Importer module

        Imports data using the dataset description files

        Args:
            data_structures: DatasetDescriptions object describing the different datasets
            use_sample: boolean indicating whether a sample can be used
            use_preprocessed_files: boolean indicating that preprocessed files can be used

        Examples:
            Example without sample and preprocessed files
            >>> from promg.modules.data_importer import Importer
            >>> # set dataset name
            >>> dataset_name = 'BPIC17'
            >>> # location of json file with dataset_description
            >>> ds_path = Path(f'json_files/{dataset_name}_DS.json')
            >>> dataset_descriptions = DatasetDescriptions(ds_path)
            >>> importer = Importer(data_structures = dataset_descriptions)
            The module to import data is returned.
            The module won't use a sample, nor the preprocessed files

            Example with sample and preprocessed files

            >>> from promg.modules.data_importer import Importer
            >>> # set dataset name
            >>> dataset_name = 'BPIC17'
            >>> # location of json file with dataset_description
            >>> ds_path = Path(f'json_files/{dataset_name}_DS.json')
            >>> dataset_descriptions = DatasetDescriptions(ds_path)
            >>> importer = Importer(data_structures = dataset_descriptions,
            >>>                     use_sample = True,
            >>>                     use_preprocessed_files = True)
            The module to import data is returned.
            The module will use the sample and the preprocessed files
            if they exist, in case they do not exist, they are created
    """

    def __init__(self, data_structures: DatasetDescriptions,
                 use_sample: bool = False, use_preprocessed_files: bool = False):
        self.connection = DatabaseConnection()
        self.structures = data_structures.structures
        self.records = SemanticHeader().records

        self.load_batch_size = 20000
        self.use_sample = use_sample
        self.use_preprocessed_files = use_preprocessed_files
        self.load_status = 0

    def _update_load_status(self):
        """
        Method to keep track of the load status of the different record nodes
        """
        self.connection.exec_query(di_ql.get_update_load_status_query,
                                   **{
                                       "current_load_status": self.load_status
                                   })
        self.load_status += 1

    def import_data(self) -> None:
        """
        Method that imports the data records into the graph database as (:Record) nodes.
        The records contain the attributes as described in the dataset descriptions.
        Method also adds the specific record labels as specified by the semantic header.

        Examples:
            >>> importer.import_data()
            The records of the dataset described in the dataset descriptions are imported as (:Record) nodes with
            appropriate attributes and labels
        """
        for structure in self.structures:
            record_constructors = self._get_record_constructors_by_labels(structure=structure)
            file_directory = structure.file_directory
            # read in all file names that match this structure
            for file_name in structure.file_names:
                # read and import the nodes
                df_log = structure.read_data_set(file_directory, file_name, use_sample=self.use_sample,
                                                 use_preprocessed_file=self.use_preprocessed_files)
                df_log["loadStatus"] = self.load_status
                self._import_nodes_from_data(df_log=df_log, file_name=file_name,
                                             record_constructors=record_constructors)

            if structure.has_datetime_attribute():
                # once all events are imported, we convert the string timestamp to the timestamp as used in Cypher
                self._reformat_timestamps(structure=structure)
                self._update_load_status()

            self._filter_nodes(structure=structure)  # filter nodes according to the structure

            self._remove_load_status_attribute()  # removes temporary properties

    @Performance.track("structure")
    def _reformat_timestamps(self, structure: DataStructure):
        """
        Method that converts the timestamps of (:Record) nodes imported according to a specific structure
        from string to DateTime or Time attribute.

        Args:
            structure: The data structure that contains the just imported nodes
        """

        datetime_formats = structure.get_datetime_formats()
        for attribute, datetime_format in datetime_formats.items():
            if datetime_format.is_epoch:
                self.connection.exec_query(di_ql.get_convert_epoch_to_timestamp_query,
                                           **{
                                               "attribute": attribute,
                                               "datetime_object": datetime_format
                                           })

            self.connection.exec_query(di_ql.get_make_timestamp_date_query,
                                       **{
                                           "attribute": attribute,
                                           "datetime_object": datetime_format,
                                           "load_status": self.load_status
                                       })

    @Performance.track("structure")
    def _filter_nodes(self, structure):
        # TODO: check function and
        for boolean in (True, False):
            attribute_values_pairs_filtered = structure.get_attribute_value_pairs_filtered(exclude=boolean)
            for name, values in attribute_values_pairs_filtered.items():
                self.connection.exec_query(di_ql.get_filter_events_by_property_query,
                                           **{
                                               "prop": name, "values": values, "exclude": boolean,
                                               "load_status": self.load_status
                                           })

    @Performance.track("structure")
    def _remove_load_status_attribute(self):
        """
        Method that removes the load status attribute from the (:Record) nodes
        """
        self.connection.exec_query(di_ql.get_finalize_import_events_query)

    @Performance.track("file_name")
    def _import_nodes_from_data(self, df_log: DataFrame, file_name: str,
                                record_constructors: List[Dict[str, Union[RecordConstructor, bool]]]):
        """
        Method that imports records from a dataframe log as (:Record) nodes and assigns the correct labels

        Args:
            df_log: The records to be imported in Dataframe format
            file_name: The file name from which the records to be imported originate from
            record_constructors: A list indicating which record labels should be (possibly) assigned to the imported
            (:Record) nodes
        """

        # start with batch 0 and increment until everything is imported
        batch = 0
        print("\n")
        pbar = tqdm(total=math.ceil(len(df_log) / self.load_batch_size), position=0)

        labels_constructor = di_ql.get_label_constructors(record_constructors)

        while batch * self.load_batch_size < len(df_log):
            pbar.set_description(f"Loading data from {file_name} from batch {batch}")

            # import the events in batches, use the records of the log
            batch_without_nans = [{k: int(v) if isinstance(v, np.integer) else v for k, v in m.items()
                                   if (isinstance(v, list) and len(v) > 0) or (not pd.isna(v) and v is not None)}
                                  for m in
                                  df_log[batch * self.load_batch_size:(batch + 1) * self.load_batch_size].to_dict(
                                      orient='records')]

            self.connection.exec_query(di_ql.get_create_nodes_by_importing_batch_query,
                                       **{
                                           "batch": batch_without_nans,
                                           "labels_constructors": labels_constructor
                                       })

            pbar.update(1)
            batch += 1
        pbar.close()

    def _get_record_constructors_by_labels(self, structure: DataStructure) -> List[
        Dict[str, Union[RecordConstructor, bool]]]:
        """
        Method to determine for the structure which record labels are optional/required for the (:Record) nodes


        Args:
            structure: The DataStructure for which the labels are checked
        """

        constructors = []
        labels = structure.labels

        # loop over all record constructions
        for record_constructor in self.records:
            # if labels are defined, determine the intersection between the labels of structure and the labels of the
            # record construction
            if labels is not None:
                intersection = list(set(labels) & set(record_constructor.record_labels))
            else:  # take the record labels of the record constructor
                intersection = record_constructor.record_labels
            if len(intersection) > 0:  # label of record constructor is in intersection or labels is not defined
                required = True
                # if there is a where condition defined, then imported (:Record) nodes might not have the record label
                # hence required = false
                if record_constructor.prevalent_record.where_condition != "":
                    required = False
                else:
                    # check whether each required attribute in the record constructor is also required according to
                    # the structure (except index, is always present)
                    for required_attribute in record_constructor.required_attributes:
                        if required_attribute == "index":
                            continue
                        if required_attribute not in structure.attributes or structure.attributes[
                            required_attribute].optional:
                            required = False
                constructors.append({"required": required, "record_constructor": record_constructor})
        return constructors
