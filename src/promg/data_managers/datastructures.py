import json
import os
import re
import warnings
import random
from pathlib import Path

from typing import List, Dict, Any, Optional
from dataclasses import dataclass

import numpy as np
import pandas as pd
from pandas import DataFrame

from .semantic_header import RecordConstructor
from ..utilities.auxiliary_functions import replace_undefined_value, create_list
from ..utilities.configuration import Configuration


@dataclass
class DatetimeObject:
    format: str
    timezone_offset: str
    convert_to: str
    is_epoch: bool
    unit: str

    def get_date_type(self):
        if self.convert_to == "ISO_DATE":
            return "DATE"
        else:
            return "DATE_TIME"

    @staticmethod
    def from_dict(obj: Any) -> 'DatetimeObject':
        if obj is None:
            return None
        _format = obj.get("format")
        _timezone_offset = replace_undefined_value(obj.get("timezone_offset"), "")
        _convert_to = "ISO_DATE"
        if re.search('[hkHK]', _format):
            _convert_to = "ISO_DATE_TIME"

        # _convert_to = str(obj.get("convert_to"))
        _is_epoch = replace_undefined_value(obj.get("is_epoch"), False)
        _unit = obj.get("unit")
        return DatetimeObject(_format, _timezone_offset, _convert_to, _is_epoch, _unit)


@dataclass
class Column:
    name: str
    dtype: str
    nan_values: List[str]
    optional: bool
    range_start: int
    range_end: int

    @staticmethod
    def from_dict(obj: Any) -> Optional['Column']:
        if obj is None:
            return None
        _name = obj.get("name")
        _dtype = obj.get("dtype")
        _nan_values = replace_undefined_value(obj.get("nan_values"), [])
        _optional = replace_undefined_value(obj.get("optional"), False)
        _range_start = obj.get("range_start")
        _range_end = obj.get("range_end")
        return Column(_name, _dtype, _nan_values, _optional, _range_start, _range_end)


@dataclass
class Attribute:
    name: str
    columns: List[Column]
    separator: str
    is_datetime: bool
    is_compound: bool
    optional: bool
    datetime_object: DatetimeObject
    na_rep_value: Any
    na_rep_columns: List[Column]
    filter_exclude_values: List[str]
    filter_include_values: List[str]
    use_filter: bool
    is_primary_key: bool
    is_foreign_key: bool

    @staticmethod
    def from_dict(obj: Any) -> Optional['Attribute']:
        if obj is None:
            return None
        _name = obj.get("name")
        _columns = create_list(Column, obj.get("columns"))
        _is_compound = len(_columns) > 1
        _optional = bool(obj.get("optional"))
        _datetime_object = DatetimeObject.from_dict(obj.get("datetime_object"))
        _is_datetime = _datetime_object is not None
        _na_rep_value = obj.get("na_rep_value")
        _na_rep_columns = create_list(Column, obj.get("na_rep_columns"))
        _separator = obj.get("separator")
        _filter_exclude_values = obj.get("filter_exclude_values")
        _filter_include_values = obj.get("filter_include_values")
        _use_filter = _filter_include_values is not None or _filter_exclude_values is not None  # default value
        _use_filter = replace_undefined_value(obj.get("use_filter"), _use_filter)
        _is_primary_key = replace_undefined_value(obj.get("is_primary_key"), False)
        _is_foreign_key = replace_undefined_value(obj.get("is_foreign_key"), False)
        return Attribute(name=_name, optional=_optional, columns=_columns, separator=_separator,
                         is_compound=_is_compound,
                         is_datetime=_is_datetime, datetime_object=_datetime_object,
                         na_rep_value=_na_rep_value, na_rep_columns=_na_rep_columns,
                         filter_exclude_values=_filter_exclude_values, filter_include_values=_filter_include_values,
                         use_filter=_use_filter, is_primary_key=_is_primary_key, is_foreign_key=_is_foreign_key)


@dataclass
class Sample:
    file_name: str
    use_random_sample: bool
    population_column: str
    size: int
    ids: List[Any]
    between: List[str]
    format: Optional[str]

    @staticmethod
    def from_dict(obj: Any, default_file_name: Optional[str] = None) -> Optional['Sample']:
        if obj is None:
            return None
        # when there is a single file specified, then we can use the default file name
        if default_file_name is not None:
            _file_name = replace_undefined_value(obj.get("file_name"), default_file_name)
        else:
            _file_name = obj.get("file_name")
        _use_random_sample = obj.get("use_random_sample")
        _population_column = obj.get("population_column")
        _size = obj.get("size")
        _ids = obj.get("ids")
        _between = obj.get("between")
        _format = obj.get("datetime_format")

        return Sample(_file_name, _use_random_sample, _population_column, _size, _ids, _between, _format)


class DataStructure:
    def __init__(self, include: bool, name: str, file_directory: str, file_names: List[str],
                 encoding: str, seperator: str, decimal: str,
                 labels: List[str], true_values: List[str], false_values: List[str],
                 add_log: bool, add_index: bool,
                 samples: Dict[str, Sample], attributes: Dict[str, Attribute],
                 split_combined_events: bool):
        self.include = include
        self.name = name
        self.file_directory = file_directory
        self.preprocessed_file_directory = os.path.join(self.file_directory, "preprocessed_files")
        self.file_names = file_names
        self.encoding = encoding
        self.seperator = seperator
        self.decimal = decimal
        self.labels = labels
        if self.labels == ["Record"]:
            self.labels = None
        self.true_values = true_values
        self.false_values = false_values
        self.add_log = add_log
        self.add_index = add_index
        self.samples = samples
        self.attributes = attributes
        self.split_combined_events = split_combined_events
        self.required_labels = None

    def __repr__(self):
        return self.name

    def has_datetime_attribute(self):
        return any([attribute.is_datetime for attribute in self.attributes.values()])
        # return "Event" in self.labels or "EventRecord" in self.labels

    def contains_composed_events(self):
        contains_composed_events = "startTimestamp" in self.get_datetime_formats() \
                                   or "completeTimestamp" in self.get_datetime_formats()
        return self.has_datetime_attribute() and contains_composed_events

    @staticmethod
    def from_dict(obj: Any) -> Optional['DataStructure']:
        if obj is None:
            return None

        _include = replace_undefined_value(obj.get("include"), True)

        if not _include:
            return None

        _name = obj.get("name")
        _path_of_executed_file = os.getcwd()
        _file_directory = os.path.join(os.getcwd(), *obj.get("file_directory").split("\\"))
        _file_names = obj.get("file_names") if obj.get("file_names") is not None else [obj.get("file_name")]
        _encoding = replace_undefined_value(obj.get("encoding"), "utf-8")
        _seperator = replace_undefined_value(obj.get("seperator"), ",")
        _decimal = replace_undefined_value(obj.get("decimal"), ".")
        _labels = obj.get("labels")
        _true_values = obj.get("true_values")
        _false_values = obj.get("false_values")
        _add_log = replace_undefined_value(obj.get("add_log"), False)
        _add_index = replace_undefined_value(obj.get("add_index"), True)

        _samples_obj = obj.get("samples") if obj.get("samples") is not None else obj.get("sample")
        if len(_file_names) == 1:  # single file name is defined
            _samples = create_list(Sample, _samples_obj, _file_names[0])
        else:
            _samples = create_list(Sample, _samples_obj)

        _samples = {sample.file_name: sample for sample in _samples}
        _attributes = create_list(Attribute, obj.get("attributes"))
        _attributes = {attribute.name: attribute for attribute in _attributes}
        _split_combined_events = replace_undefined_value(obj.get("split_combined_events"), False)

        return DataStructure(_include, _name, _file_directory, _file_names, _encoding, _seperator, _decimal,
                             _labels, _true_values, _false_values, _add_log, _add_index,
                             _samples, _attributes, _split_combined_events)

    def get_primary_keys(self):
        return [attribute_name for attribute_name, attribute in self.attributes.items() if attribute.is_primary_key]

    def get_primary_keys_as_attributes(self):
        primary_keys = self.get_primary_keys()
        primary_key_with = [f"n.{primary_key} as {primary_key}" for primary_key in primary_keys]
        primary_key_string = ", ".join(primary_key_with)
        return primary_key_string

    def get_label_string(self):
        return ":".join(self.labels)

    def get_foreign_keys(self):
        return [attribute_name for attribute_name, attribute in self.attributes.items() if attribute.is_foreign_key]

    def get_dtype_dict(self):
        dtypes = {}
        for attribute in self.attributes.values():
            for column in attribute.columns:
                if column.dtype is not None:
                    if column.name not in dtypes:
                        dtypes[column.name] = column.dtype
                    elif column.dtype != dtypes[column.name]:
                        warnings.warn(
                            f"Multiple dtypes ({column.dtype} != {dtypes[column.name]}) "
                            f"defined for {column.name}")
        return dtypes

    def get_required_columns(self):
        required_columns = set()
        for attribute in self.attributes.values():
            # add column names to the required columns
            required_columns.update([x.name for x in attribute.columns])
            required_columns.update([x.name for x in attribute.na_rep_columns])

        return list(required_columns)

    def create_sample(self, file_name, df_log):
        if self.samples is None or file_name not in self.samples:
            warnings.warn(f"No sample population has been defined for {self.name}")
            return df_log

        sample = self.samples[file_name]
        sample_column = sample.population_column
        if sample.use_random_sample:
            random_selection = random.sample(df_log[sample_column].unique().tolist(), k=sample.size)
            df_log = df_log[df_log[sample_column].isin(random_selection)]
        else:
            if sample.ids is not None:
                random_selection = sample.ids
                df_log = df_log[df_log[sample_column].isin(random_selection)]
            elif sample.between is not None:
                start_date = sample.between[0]
                end_date = sample.between[1]
                df_log['date_time_conv'] = pd.to_datetime(df_log[sample_column], format=sample.format)
                df_log = df_log.loc[df_log["date_time_conv"].between(start_date, end_date)]
                df_log = df_log.drop(columns=['date_time_conv'])

        df_log = df_log.reset_index(drop=True)
        return df_log

    @staticmethod
    def replace_nan_values_based_on_na_rep_columns(df_log, attribute):
        if len(attribute.na_rep_columns) != len(attribute.columns):
            raise Exception(
                f"Na_rep_columns does not have the same size as columns for attribute {attribute.name}")
        else:  # they are the same size
            for i, na_rep_column in zip(range(len(attribute.na_rep_columns)), attribute.na_rep_columns):
                attribute_name = f"{attribute.name}_{i}"
                df_log[attribute_name].fillna(df_log[na_rep_column.name], inplace=True)

        return df_log

    @staticmethod
    def replace_nan_values_based_on_na_rep_value(df_log, attribute):
        for i in range(len(attribute.columns)):
            attribute_name = f"{attribute.name}_{i}"
            df_log[attribute_name].fillna(attribute.na_rep_value, inplace=True)

        return df_log

    @staticmethod
    def replace_nan_values_with_unknown(df_log, attribute):
        column: Column
        for i, column in zip(range(len(attribute.columns)), attribute.columns):
            attribute_name = f"{attribute.name}_{i}"
            if not column.optional:
                try:
                    df_log[attribute_name].fillna("Unknown", inplace=True)
                except:
                    df_log[attribute_name].fillna(-1, inplace=True)
        return df_log

    @staticmethod
    def create_compound_attribute(df_log, attribute):
        compound_column_names = [x.name for x in attribute.columns]
        df_log[attribute.name] = df_log[compound_column_names].apply(
            lambda row: attribute.separator.join([value for value in row.values.astype(str) if
                                                  not (value == 'nan' or value != value)]), axis=1)
        return df_log

    @staticmethod
    def combine_attribute_columns(df_log, attribute):
        compound_attribute_names = [f"{attribute.name}_{i}" for i in range(len(attribute.columns))]
        if attribute.is_compound:
            df_log[f"{attribute.name}_attribute"] = df_log[compound_attribute_names].apply(
                lambda row: attribute.separator.join([value for value in row.values.astype(str) if
                                                      not (value == 'nan' or value != value)]), axis=1)
        else:
            df_log[f"{attribute.name}_attribute"] = df_log[f"{attribute.name}_0"]
        df_log = df_log.drop(columns=compound_attribute_names)
        return df_log

    @staticmethod
    def create_attribute_columns(df_log, attribute):
        for i, column in zip(range(len(attribute.columns)), attribute.columns):
            attribute_name = f"{attribute.name}_{i}"
            df_log[attribute_name] = df_log[column.name]
            if column.range_start is not None or column.range_end is not None:
                df_log[attribute_name] = df_log[attribute_name].str[column.range_start:column.range_end]
        return df_log

    @staticmethod
    def replace_with_nan(df_log, attribute):
        for i, column in zip(range(len(attribute.columns)), attribute.columns):
            attribute_name = f"{attribute.name}_{i}"
            for nan_value in column.nan_values:
                df_log[attribute_name] = df_log[attribute_name].replace(nan_value, np.nan, regex=False)
        return df_log

    def preprocess_according_to_attributes(self, df_log):
        # loop over all attributes and check if they should be created, renamed or imputed
        for attribute in self.attributes.values():
            df_log = DataStructure.create_attribute_columns(df_log, attribute)
            df_log = DataStructure.replace_with_nan(df_log, attribute)
            if len(attribute.na_rep_columns) > 0:  # impute values in case of missing values
                df_log = DataStructure.replace_nan_values_based_on_na_rep_columns(df_log, attribute)
            if attribute.na_rep_value is not None:
                df_log = DataStructure.replace_nan_values_based_on_na_rep_value(df_log, attribute)

            df_log = DataStructure.combine_attribute_columns(df_log, attribute)

        return df_log

    def split_df_log_into_combined_events(self, df_log: DataFrame):
        df_log["idx"] = df_log.reset_index().index
        if "timestamp" in self.attributes:
            raise ImportError(
                "Combined events cannot be split since an attribute with name timestamp is already defined")

        if "startTimestamp" in self.attributes and "completeTimestamp" in self.attributes:
            df_log_start = df_log.drop(columns=["completeTimestamp"])
            df_log_start["lifecycle"] = "start"
            df_log_start = df_log_start.rename(columns={"startTimestamp": "timestamp"})

            df_log_end = df_log.drop(columns=["startTimestamp"])
            df_log_end["lifecycle"] = "complete"
            df_log_end = df_log_end.rename(columns={"completeTimestamp": "timestamp"})

            df_log = pd.concat([df_log_start, df_log_end])
        elif "startTimestamp" in self.attributes:
            df_log["lifecycle"] = "start"
            df_log = df_log.rename(columns={"startTimestamp": "timestamp"})
        elif "completeTimestamp" in self.attributes:
            df_log["lifecycle"] = "complete"
            df_log = df_log.rename(columns={"completeTimestamp": "timestamp"})

        df_log = df_log.sort_values(by=["timestamp", "idx"])
        df_log = df_log.drop(columns=["idx"])
        return df_log

    def update_attributes(self):
        if "startTimestamp" in self.attributes and "completeTimestamp" in self.attributes:
            start_dt_format = self.get_datetime_formats()["startTimestamp"].format
            complete_dt_format = self.get_datetime_formats()["completeTimestamp"].format
            if start_dt_format != complete_dt_format:
                raise ValueError("startTimestamp and completeTimestamp have a different format")

            start_attribute = self.attributes["startTimestamp"]
            start_attribute.name = "timestamp"
            self.attributes["timestamp"] = start_attribute
            del self.attributes["startTimestamp"]
            del self.attributes["completeTimestamp"]


        elif "startTimestamp" in self.attributes:
            start_attribute = self.attributes["startTimestamp"]
            start_attribute.name = "timestamp"

            self.attributes["timestamp"] = start_attribute
            del self.attributes["startTimestamp"]
        elif "completeTimestamp" in self.attributes:
            complete_attribute = self.attributes["completeTimestamp"]
            complete_attribute.name = "timestamp"

            self.attributes["timestamp"] = complete_attribute
            del self.attributes["completeTimestamp"]

    def check_if_required_attributes_are_present(self, record_constructor):
        # loop over all required attributes of the record constructor
        # --> check whether they are required by self
        required = True
        for required_attribute in record_constructor.required_attributes:
            if required_attribute == "index":  # records of an always have an index, so records will have
                # the required attributes = index
                continue
            if required_attribute not in self.attributes or self.attributes[required_attribute].optional:
                # if the required attribute does not occur in the self.attributes or the attribute is
                # optional, then the record_constructor labels are not required
                required = False
        return required

    def determine_required_labels(self, records: List["RecordConstructor"]):
        # determine the required labels for the records defined by this datastructure
        # therefore, we have to check for all record constructors in records
        # whether the required attributes are present by the dataset description definition

        # initially, self.required_labels might be None, hence we need to make it an empty list.
        if self.required_labels is None:
            self.required_labels = []

        # loop over all record_constructors in records
        for record_constructor in records:
            # check whether self.labels appear in the record constructor
            if self.labels_appear_in_record_constructor(record_constructor):
                if record_constructor.prevalent_record.where_condition != "":
                    # a where condition is specified, then not all records require the record_constructor labels
                    required = False
                else:
                    required = self.check_if_required_attributes_are_present(record_constructor)
                if required:
                    # if required = true, then the record_constructor labels are required labels of self
                    self.required_labels.extend(record_constructor.record_labels)
        self.required_labels = list(set(self.required_labels))

    def get_required_labels(self, records: List["RecordConstructor"]):
        if self.required_labels is None:
            self.determine_required_labels(records)
        return self.required_labels

    def get_required_labels_str(self, records: List["RecordConstructor"]):
        """Method to convert the required labels determined by records to a string seperated by :
        :param records: the required
        """
        if self.required_labels is None:
            self.determine_required_labels(records)
        required_labels_w_records = self.required_labels[:]
        required_labels_w_records.insert(0, "Record")
        required_labels_str = ":".join(required_labels_w_records)
        return required_labels_str

    def read_data_set(self, file_name, use_sample, store_preprocessed_file=True,
                      use_preprocessed_file=False):
        preprocessed_file_name = self._get_preprocessed_file_name(file_name, use_sample)
        df_log = None
        if use_preprocessed_file:
            df_log = self.read_preprocessed_df_log(preprocessed_file_name)
        if df_log is None:  # no preprocessed file was read
            df_log = self.read_df_log(file_name, use_sample)
            if store_preprocessed_file:
                self.store_df_log(df_log, preprocessed_file_name)

        return df_log

    def read_df_log(self, file_name, use_sample):
        df_log = self.prepare_event_data_sets(file_name, use_sample)
        return df_log

    def read_preprocessed_df_log(self, preprocessed_file_name):
        preprocessed_file_path = os.path.join(self.preprocessed_file_directory, preprocessed_file_name)
        # first try to read preprocessed file
        if not os.path.exists(preprocessed_file_path):
            warning_message = f"No preprocessed file {preprocessed_file_name} found, preprocessed the file " \
                              f"instead"
            warnings.warn(warning_message)
            return None
        else:  # use_preprocessed_file and file exists
            df_log = pd.read_pickle(preprocessed_file_path)
        return df_log

    def store_df_log(self, df_log, preprocessed_file_name):
        # only store file if we did not read from the stored file
        os.makedirs(self.preprocessed_file_directory, exist_ok=True)

        preprocessed_file_path = os.path.join(self.preprocessed_file_directory, preprocessed_file_name)
        df_log.to_pickle(preprocessed_file_path)

    def _get_preprocessed_file_name(self, file_name, use_sample):
        # change extension from csv to pkl and add sample in case of sample
        sample_is_used = use_sample and len(self.samples) > 0
        preprocessed_file_name = f"{file_name[:-4]}_sample.pkl" if sample_is_used else f"{file_name[:-4]}.pkl"
        return preprocessed_file_name

    @staticmethod
    def create_record_id_column(df_log, file_name):
        record_id_column = df_log.index
        record_id_column = record_id_column.astype(str) + "_" + file_name[:-4]
        return record_id_column

    def prepare_event_data_sets(self, file_name, use_sample):
        dtypes = self.get_dtype_dict()
        required_columns = self.get_required_columns()

        true_values = self.true_values
        false_values = self.false_values

        if file_name.endswith('.csv'):
            df_log: DataFrame = pd.read_csv(os.path.join(self.file_directory, file_name), keep_default_na=True,
                                            usecols=required_columns, dtype=dtypes, true_values=true_values,
                                            false_values=false_values, sep=self.seperator, decimal=self.decimal,
                                            encoding=self.encoding)
        else:
            raise TypeError(f"The file extension of {file_name} is not implemented. Use .csv.")

        if use_sample and self.has_datetime_attribute():
            df_log = self.create_sample(file_name, df_log)

        df_log = self.preprocess_according_to_attributes(df_log)

        # all columns have been renamed to or constructed with the name attribute,
        # hence only keep those that match with a name attribute
        required_attributes = list([f"{attribute_name}_attribute" for attribute_name in self.attributes.keys()])
        required_attributes_mapping = {f"{attribute_name}_attribute": f"{attribute_name}" for attribute_name in
                                       self.attributes.keys()}
        df_log = df_log[required_attributes]
        df_log = df_log.rename(columns=required_attributes_mapping)

        if self.split_combined_events:
            df_log = self.split_df_log_into_combined_events(df_log)
            self.update_attributes()

        if self.add_log:
            df_log["log"] = file_name

        if self.add_index:
            df_log["index"] = df_log.index

        # drop all columns with only nan values
        df_log = df_log.dropna(how='all', axis=1)  # drop all columns in which all values are nan (empty)
        df_log = df_log.dropna(how='all')  # drop all columns in which all values are nan (empty)

        df_log["recordId"] = self.create_record_id_column(df_log, file_name)

        return df_log

    def determine_optional_labels_in_log(self, df_log, records):
        df_log["labels"] = ""

        if self.required_labels is None:
            self.determine_required_labels(records)

        if set(self.required_labels) == set(self.labels):
            return df_log

        for record_constructor in records:
            if self.labels_appear_in_record_constructor(record_constructor):
                if set(record_constructor.record_labels) <= set(self.required_labels):
                    continue  # record constructor only contains required labels

                # one of possible labels appear in record constructor
                should_have_label = self.all_required_attributes_are_present_in_df_log(df_log, record_constructor)
                if should_have_label.any():  # check whether there is still a row that can have the label
                    should_have_label = should_have_label & self.is_where_condition_satisfied(
                        df_log, record_constructor)
                if should_have_label.any():
                    for label in record_constructor.record_labels:
                        df_log.loc[should_have_label, "labels"] += ":" + label

        return df_log

    def labels_appear_in_record_constructor(self, record_constructor):
        if self.labels is not None:
            # check whether the labels have overlap with the labels of the record_constructor
            intersection = list(set(self.labels) & set(record_constructor.record_labels))
        else:
            # no labels are defined, hence we consider all labels of the record constructor
            intersection = record_constructor.record_labels

        return len(intersection) > 0

    def all_required_attributes_are_present_in_df_log(self, df_log, record_constructor):
        required_attributes_are_present = pd.Series(True, index=np.arange(len(df_log)), name='present')
        for required_attribute in record_constructor.required_attributes:
            if required_attribute == "index" or required_attribute == "log":
                continue
            if required_attribute in self.attributes:
                if self.attributes[required_attribute].optional:
                    # if the required attribute is optional in the structure, check whether the required attribute is
                    # not null
                    required_attributes_are_present = required_attributes_are_present & df_log[
                        required_attribute].notnull()
                # else the required attributes is required in the structure, hence nothing changes
            else:  # the required attribute is missing in the structure -> hence not all are present
                required_attributes_are_present = pd.Series(False, index=np.arange(len(df_log)), name='present')
                # since for all rows, at least an attribute is missing, we can return
                return required_attributes_are_present

        return required_attributes_are_present

    @staticmethod
    def is_where_condition_satisfied(df_log, record_constructor):
        # TODO split where_condition
        where_condition = record_constructor.prevalent_record.where_condition
        where_condition_satisfied = pd.Series(True, index=np.arange(len(df_log)), name='satisfied')
        if where_condition == "":
            return where_condition_satisfied
        where_conditions = where_condition.split("AND")
        for condition in where_conditions:
            stripped_condition = condition.strip()
            if "=" in condition:
                condition_list = stripped_condition.split("=")
                column_name = condition_list[0].strip()
                if "." in column_name:
                    column_name = column_name.split(".")[1].strip()
                column_value = condition_list[1].strip().strip('"')
                where_condition_satisfied = where_condition_satisfied & (
                        df_log[column_name].astype("string") == column_value)
            elif "STARTS WITH" in condition:
                condition_list = stripped_condition.split("STARTS WITH")
                column_name = condition_list[0].strip()
                if "." in column_name:
                    column_name = column_name.split(".")[1].strip()
                column_value = condition_list[1].strip().strip('"')
                where_condition_satisfied = where_condition_satisfied & df_log[
                    column_name].str.startswith(column_value)
            elif "ENDS WITH" in condition:
                condition_list = stripped_condition.split("ENDS WITH")
                column_name = condition_list[0].strip()
                if "." in column_name:
                    column_name = column_name.split(".")[1].strip()
                column_value = condition_list[1].strip().strip('"')
                where_condition_satisfied = where_condition_satisfied & df_log[
                    column_name].str.endswith(column_value)
        return where_condition_satisfied

    def get_datetime_formats(self) -> Dict[str, DatetimeObject]:
        datetime_formats = {}

        for attribute_name, attribute in self.attributes.items():
            if attribute.is_datetime:
                datetime_formats[attribute_name] = attribute.datetime_object

        return datetime_formats

    def get_attribute_value_pairs_filtered(self, exclude: bool) -> Dict[str, List[str]]:
        attribute_value_pairs = {}
        for attribute_name, attribute in self.attributes.items():
            if attribute.use_filter:
                attribute_value_pairs[attribute_name] \
                    = attribute.filter_exclude_values if exclude else attribute.filter_include_values

        return attribute_value_pairs


class DatasetDescriptions:
    def __init__(self, config: Configuration):
        path = config.dataset_description_path

        random.seed(1)
        with open(path, encoding='utf-8') as f:
            json_event_tables = json.load(f)

        self.structures = [DataStructure.from_dict(item) for item in json_event_tables]
        self.structures = [item for item in self.structures if item is not None]

    def get_structure_name_file_mapping(self):
        # request the file names per structure
        file_names = {}
        for structure in self.structures:
            file_names[structure.name] = structure.file_names
        return file_names

    def get_files_list(self):
        # request all file names specified in self.structures in a list
        files = []
        for structure in self.structures:
            files.extend(structure.file_names)
        return files
