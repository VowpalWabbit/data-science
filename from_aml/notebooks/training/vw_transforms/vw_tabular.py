# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import numpy as np
import pandas as pd
import json
from collections import OrderedDict
from enum import Enum
import csv
import gzip
import numbers
import sys


from .converter_common import namespacify
from .converter_common import process_config_columns, process_event_links, get_match
from .converter_common import ConfigError
from .converter_common import create_default_config


def _coalesce_actions(old, new):
    '''Combine features across two versions of the same action, generating a list of cases where the same feature has a different
    value in each version.  Actions are represented as dictionaries mapping feature to value.  In cases where the same feature
    maps to two different values, use the new value.
    
    Parameters:
    old -- initial version of the action, as a dict.  The key-value pairs from new will be added to this dict.
    new -- new version of the action, as a dict
    
    Returns:
    A list of conflicts, each of which is a triple of (key, old_feature_value, new_feature_value)
    '''
    conflicts = []
    for key, new_value in new.items():
        if key in old:
            old_value = old[key]
            if old_value != new_value:
                conflicts.append((key, old_value, new_value))
        old[key] = new_value
    return conflicts


def _actions_differ(old, new, coalesce_nulls):
    '''Determine if two versions of an action (represented as dicts) differ, meaning that both versions provide different values for
    the same feature.  Features starting with '_' are ignored.
    
    Parameters:
    old -- original version of the action
    new -- new version of the action
    coalesce_nulls -- determines how this function will behave when one action version contains a feature which is undefined (null) in
                      the other.  If True, such cases will not be treated as differences.  If False, they will.
    
    Returns:
    True if the versions differ, or False if not
    '''
    for key, new_value in new.items():
        if isinstance(key, str) and key.startswith('_'):
            continue
        if key in old:
            if old[key] != new_value:
                return True
        else:
            if not coalesce_nulls:
                return True
    if not coalesce_nulls:
        for key in old.keys():
            if isinstance(key, str) and key.startswith('_'):
                continue
            if key not in new:
                return True
    return False


def _extract_features(feature_dict, column_dict, extract_dict):
    '''Extract a set of columns (and their values) from a set of raw features, using a column dict to determine how to map 
    individual features to columns.  Results are accumulated into extract_dict.
    
    Parameters:
    feature_dict -- Features, expressed as a mapping of (namespace,feature) --> value.  Toplevel features will have an empty string
                   for the namespace.
    column_dict -- Mapping identifying how to convert (namespace,feature) pairs into columns.  The mapping may contain three
                  types of entries:
                  Single feature mappings map (namespace,feature) --> [column_name].  The single feature will be extracted.
                  Namespace mappings map namespace --> [(feature_wildcard, column_wildcard)].
                      Every feature in the namespace that matches feature_wildcard will be extracted.  The column name is computed as follows:
                        Match the feature against feature_wildcard, and determine the value of '*'.
                        Replace '*' in the column_wildcard based on this value.
                  Full mappings map () --> [(namespace_wildcard, feature_wildcard, column_wildcard)].
                      Every matching feature in every matching will be extracted.  Features and namespaces match if they match their respective
                      wildcards.  To construct the column name, first we extract the portions of the namespace and feature that correspond to '*'
                      in their wildcards.  Then we replace '**' in column_wildcard with <n>.<f> where <n> is the value extracted from the
                      namespace and <f> is the value extracted from the feature, and '.' is the separator character that occurs right before the
                      '**' in column_wildcard.
                  The right-hand side of each mapping is a list.  All mappings in the list will be applied.
                  Multiple mappings may match the same feature, in which case the feature will be mapped into multiple columns.
                  Note: Full mappings will not match features in the default namespace.  Use a namespace or single feature mapping for these.
    extract_dict -- Results will be placed here.
    '''
    for (namespace, feature), value in feature_dict.items():
        if (namespace, feature) in column_dict:
            for column in column_dict[(namespace, feature)]:
                extract_dict[column] = value
        if namespace in column_dict:
            for (feature_wildcard, column_wildcard) in column_dict[namespace]:
                feature_extract = get_match(feature, feature_wildcard)
                if feature_extract:
                    column = column_wildcard.replace('*', feature_extract)
                    extract_dict[column] = value
        if () in column_dict:
            for (namespace_wildcard, feature_wildcard, column_wildcard) in column_dict[()]:
                namespace_extract = get_match(namespace, namespace_wildcard)
                if not namespace_extract:
                    continue
                feature_extract = get_match(feature, feature_wildcard)
                if not feature_extract:
                    continue
                family_name = column_wildcard[:-3]
                separator = column_wildcard[-3:-2]
                column = family_name + separator + namespace_extract + separator + feature_extract
                extract_dict[column] = value


def _extract_toplevel_fields(feature_dict, column_dict, extract_dict):
    '''Extract toplevel fields from the given feature_dict, using column_dict to map feature names to column names.
    Accumulate the results into extract_dict.
    
    Parameters:
    feature_dict -- Snippet of a dsjson-formatted file.  Toplevel features are any fields mapped directly to string values,
                    whether or not they begin with '_'.
    column_dict -- Mapping identifying how to convert toplevel features into columns.  This is a simple mapping from
                   column_name to feature_name.
    extract_dict -- Results will be placed here.
    '''
    for field, value in feature_dict.items():
        if (isinstance(value, str) or isinstance(value, numbers.Number)) and field in column_dict:
            column_name = column_dict[field]
            extract_dict[column_name] = value

    
class ExtractorTable:
    '''ExtractorTable holds the attributes and data of a table used for extraction.  E.g. Event Table and Action Table.
    '''
    
    def __init__(self, table_config):
        '''Create an instance of ExtractorTable.
        
        Parameters:
        table_config -- configuration section for this table.  E.g. the "event_table" or "action_table" section.
        '''
        self.toplevel_dict = None
        self.column_dict = None
        self.primary_key = None
        self.primary_key_feature = None
        
        self.primary_key = table_config['primary_key'] if 'primary_key' in table_config else None
        if 'columns' not in table_config and 'patch' not in table_config:
            raise ConfigError(config=table_config, message='table config must specify "patch" or "columns"')
        if 'patch' in table_config:
            self.toplevel_dict, self.column_dict, self.primary_key_feature = \
                process_config_columns(table_config['patch'], self.primary_key)
        else:
            self.toplevel_dict, self.column_dict, self.primary_key_feature = (OrderedDict(), OrderedDict(), None)
        if 'columns' in table_config:
            add_toplevel_dict, add_column_dict, add_primary_key_feature = \
                process_config_columns(table_config['columns'], self.primary_key)
            self.toplevel_dict.update(add_toplevel_dict)
            self.column_dict.update(add_column_dict)
            self.primary_key_feature = add_primary_key_feature if add_primary_key_feature else self.primary_key_feature

        if self.primary_key_feature is None:
            self.primary_key_feature = self.primary_key


class TabularConverter:
    '''TabularConverter is a class used to convert data from dsjson format into a standard tabular format of rows and columns,
    with options to generate CSV files and Pandas Dataframes.  More specifically, a single dsjson dataset will be converted into
    two tables: an Events table and an Actions table.
    
    TabularConverter has two major modes, depending on the value of the full_transform parameter.
    * In Full Transform mode (full_transform=True), TabularConverter will produce Events and Actions tables containing enough data to
      reconstruct a new .dsjson file equivalent to the original.
    * In Extract mode (full_transform=False), TabularConverter will extract only feature columns and keys.  This is enough information to
      patch updated values back into the original .dsjson file, but not enough information to reconstruct a .dsjson file from scratch.
    
    TabularConverter supports both Contextual Bandit and Conditional Contextual Bandit data, but Full Transform mode is only supported
    for Contextual Bandit.
    
    TabularConverter makes the following assumptions about all .djson data it processes:
    * Actions all appear in a single '_multi' list that appears under the toplevel 'c' element.
    * Every action has a unique identifier.  By default this is specified by a '_tag' attribute on that action, although it may be configured
      via the action_id parameter.
      
    In addition, to use TabularConverter in Full Transform mode (with full_transform set to True), the following must be true:
    * Each dsjson row contains action ranking and probability data ('a' and 'p' vectors).
    * All label fields are defined on each dsjson row ('_label_Action', '_label_cost', '_label_probability').
    
    TabularConverter is fully compatible with data produced by the Azure Personalizer Service, which follows all of the assumptions above.
    
    The key used to join the Actions table back to the Events table depends on how you've set changing_actions as follows:
    If changing_actions is false, the tables will be joined using the Action's unique identifier.
    If changing_actions is true, the converter will generate a new (surrogate) key to represent each Action version and the tables will be joined on this key.
    '''
    def __init__(self, changing_actions=None, coalesce_nulls=True, action_id=None, full_transform=None, write_every=10000,
                 config=None):
        '''Create a new TabularConverter instance.
        Parameters:
        coalesce_nulls -- (default True).  True to overwrite Null values in Action rows, even if changing_actions=True.  False to treat Nulls as a distinct
                          action feature value, even if this forces creation of a new Action row.
        write_every -- (default 10000).  Output a line of status every increment of this many rows.
        config -- (default None).  Configuration to use, as a dict.  If a configuration is provided, all legacy parameters will be ignored.
                  
        Legacy Parameters.  These will be used only if no config is specified:
        changing_actions -- (default False).  True to create a new Action row every time any action features change.  False to overwrite action features
                            if they change.  Also, setting this to True will create a Key column in the generated Action table along with corresponding
                            Action.rank._key columns in the Event table.    
        action_id -- (default '_tag').  Action feature to use as an identifier.  The default is '_tag'.
                     May be either '_tag', a single feature, or a tuple of (namespace,feature).
                     If action_id is a single feature then the default namespace will be used.
        full_transform -- (default True).  If True, include enough information in the Events and Actions table to reconstruct the source dsjson.  If False,
                          limit the Events table to feature columns and the label key.  Only False is supported for Conditional Contextual Bandit data.
        '''        
        self._write_every = write_every
        self._coalesce_nulls = coalesce_nulls
        self._action_id = action_id
        self._action_surrogate = 0

        self._config = None
        self._event_table = None
        self._event_links_dict = None
        self._action_table = None
        self._changing_actions = False
        self._action_surrogate_column = None
        self._action_latest_column = None
        self._action_timestamp_column = None
        self._full_transform = False
        
        # Set legacy parameter defaults, or fail if any are provided when using a config
        if config is None:
            if changing_actions is None:
                changing_actions = False
            if full_transform is None:
                full_transform = True
            if action_id is None:
                action_id = '_tag'
        else:
            if changing_actions is not None or full_transform is not None or action_id is not None:
                raise ValueError('Must not set legacy parameters changing_actions, full_transform, or action_id when setting config')
        
        # If no config is provided, create a default config based on options
        if config is None:
            self._config = create_default_config(full_transform, changing_actions, action_id)
        else:
            self._config = config
        
        if 'event_table' in self._config:
            self._event_table = ExtractorTable(self._config['event_table'])
            if 'links' in self._config['event_table']:
                self._event_links_dict = process_event_links(self._config['event_table']['links'])

        if 'action_table' in self._config:
            self._action_table = ExtractorTable(self._config['action_table'])
            
            # Turn on Changing Actions is there are toplevel mappings for _surrogate and _latest.  _timestamp is optional.
            if '_surrogate' in self._action_table.toplevel_dict or '_latest' in self._action_table.toplevel_dict:
                if '_surrogate' not in self._action_table.toplevel_dict or '_latest' not in self._action_table.toplevel_dict:
                    raise ConfigError(config=self._config, message='action_table columns must map to both _surrogate and _latest if they map to either one')
                self._changing_actions = True
                self._action_surrogate_column = self._action_table.toplevel_dict['_surrogate']
                self._action_latest_column = self._action_table.toplevel_dict['_latest']
                if '_timestamp' in self._action_table.toplevel_dict:
                    self._action_timestamp_column = self._action_table.toplevel_dict['_timestamp']
        
        if 'event_table' in self._config and 'action_table' in self._config and self._event_links_dict:
            self._full_transform = True
        
        self._reset()
    
    
    def convert_to_csv(self, input_files, events_file=None, actions_file=None):
        '''Simple conversion.  This converter reads multiple input files and generates a single CSV each
        for events and actions.  It buffers all data in memory before writing, and so will not scale to
        huge files.
        
        Parameters:
        input_files -- single file or list of input files to convert
        events_file -- (optional) output filename for events file (in csv format)
        actions_file -- (optional) output filename for actions file (in csv format)
        '''
        self._reset()
        self._read_files(input_files)
        self._write_batch(events_file, actions_file)


    def convert_to_pandas(self, input_files):
        '''Converter to Pandas dataframes.  This converter reads multiple input files and returns up to two
        Pandas dataframes, one with Events and one with Actions.  The Events dataframe will be
        indexed by EventID.  The Actions dataframe will be indexed by Key (if modeling changing actions)
        or Tag (if not).
        
        Parameters:
        input_files -- single file or list of input files to convert
        
        Returns:
        events_dataframe -- Pandas dataframe holding Events, if the converter config specifies an event table only.
        actions_dataframe -- Pandas dataframe holding Actions, if the converter config specifies an action table only.
        (events_dataframe, actions_dataframe) -- Tuple of Events and Actions dataframes, if the converter specifies both tables.
        '''
        self._reset()
        self._read_files(input_files)
        events_dataframe = self._get_events_dataframe()
        actions_dataframe = self._get_actions_dataframe()
        if events_dataframe is not None and actions_dataframe is not None:
            return (events_dataframe, actions_dataframe)
        elif events_dataframe is not None:
            return events_dataframe
        elif actions_dataframe is not None:
            return actions_dataframe
        else:
            return None

    
    def convert_streaming(self, dsjson_files, events_file, events_column_file, actions_file, actions_column_file):
        '''Streaming conversion.  This converter does not hold more than one event in memory at once,
        although it still keeps all active actions in memory.  (If modeling changing actions,
        actions get written out to disk as soon as they become inactive).
        To make this possible, it generates ragged CSV files for both events and actions, and outputs 
        column names into separate files.
        
        Parameters:
        input_files -- list of input files to convert
        events_file -- output filename for events file (in csv format)
        events_column_file -- output filename for the file to contain event column names, in a single line of csv
        actions_file -- output filename for actions file (in csv format)
        actions_column_file -- output filename for the file to contain action column names, in a single line of csv'''
        self._reset()
        with open(actions_file, 'w', newline='') as csv_actions_file:
            csv_actions_writer = csv.writer(csv_actions_file, delimiter=',', quotechar='\\', quoting=csv.QUOTE_MINIMAL)
            action_rows = 0
            event_rows = 0
            
            with open(events_file, 'w', newline='') as csv_events_file:
                csv_events_writer = csv.writer(csv_events_file, delimiter=',', quotechar='\\', quoting=csv.QUOTE_MINIMAL)
                for dsjson_file in dsjson_files:
                    if self._write_every:
                        print(f'Reading file {dsjson_file}')
                    with gzip.open(dsjson_file, 'rt', encoding='utf8') if dsjson_file.endswith('.gz') else open(dsjson_file, 'r', encoding="utf8") as vw_file:
                        for line in vw_file:
                            try:
                                dsjson_row = json.loads(line.strip())
                                if 'c' in dsjson_row:
                                    self._read_row(dsjson_row)
                                    self._write_event_row(self._event_data[0], csv_events_writer)
                                    event_rows += 1
                                    self._event_data = []
                                
                                    # Write actions in the archive and dump the archive
                                    action_rows += self._write_actions(self._action_data_archive, csv_actions_writer)
                                    self._action_data_archive = []
                                else:
                                    self._error_count += 1
                            except json.JSONDecodeError:
                                self._error_count += 1
                    # Output the total number of rows regardless
                    self._write_progress(1)
            
            # Remap and write event column names
            with open(events_column_file, 'w', newline='') as csv_events_column_file:
                csv_events_column_writer = csv.writer(csv_events_column_file, delimiter=',', quotechar='\\', quoting=csv.QUOTE_MINIMAL)
                csv_events_column_writer.writerow(self._get_event_column_names())
                
                if self._write_every:
                    print(f'Wrote {event_rows} rows and {len(self._event_column_names.keys())} columns to {events_file}') 
        
            # Write actions: remaining archive first then all current actions
            action_rows += self._write_actions(self._action_data_archive, csv_actions_writer)
            action_rows += self._write_actions(self._action_data.values(), csv_actions_writer)
        
        # Action column names
        with open(actions_column_file, 'w', newline='') as csv_actions_column_file:
            csv_actions_column_writer = csv.writer(csv_actions_column_file, delimiter=',', quotechar='\\', quoting=csv.QUOTE_MINIMAL)
            csv_actions_column_writer.writerow(self._get_action_column_names())
            
        if self._write_every:
            print(f'Wrote {action_rows} rows and {len(self._action_column_names.keys())} columns to {actions_file}')

            
    def _reset(self):
        self._event_row_count = 0
        self._error_count = 0
        self._conflict_count = 0        
        self._action_column_names = OrderedDict()
        self._event_column_names = OrderedDict()
        self._begin_batch()

            
    def _write_progress(self, write_every):
        if write_every and self._event_row_count % write_every == 0:
            print(f'{self._event_row_count} rows, {self._error_count} errors.')

            
    def _read_row(self, dsjson_row):
        # Handle actions (if an action_table config was specified)
        if self._action_table:
            actions = self._extract_actions_from_dsjson(dsjson_row)
            for action in actions:
                action_primary_key_value = action[self._action_table.primary_key]
                old_action = self._action_data[action_primary_key_value] if action_primary_key_value in self._action_data else None
                if self._changing_actions:
                    if old_action:
                        # Existing action.  Determine if any values changed and generate a new action row if needed
                        if _actions_differ(old_action, action, self._coalesce_nulls):
                            old_action[self._action_latest_column] = 0
                            self._action_surrogate += 1
                            action[self._action_surrogate_column] = self._action_surrogate
                            if 'Timestamp' in dsjson_row and self._action_timestamp_column:
                                action[self._action_timestamp_column] = dsjson_row['Timestamp']
                            action[self._action_latest_column] = 1
                        
                            # Move the old action into the archive and replace with the new action
                            self._action_data_archive.append(old_action)
                            self._action_data[action_primary_key_value] = action
                    else:
                        # Brand new action
                        self._action_surrogate += 1
                        action[self._action_surrogate_column] = self._action_surrogate
                        if 'Timestamp' in dsjson_row and self._action_timestamp_column:
                            action[self._action_timestamp_column] = dsjson_row['Timestamp']
                        action[self._action_latest_column] = 1
                        self._action_data[action_primary_key_value] = action                       
                else:
                    if old_action:
                        # Replace and log any changed action features in-place
                        conflicts = _coalesce_actions(old_action, action)
                        for (key, old_value, new_value) in conflicts:
                            if self._write_every and self._conflict_count < 100:
                                print(f'Conflict! Action {action_primary_key_value}.  Column {key}.  Expected {old_value} found {new_value}')
                            self._conflict_count += 1
                    else:
                        self._action_data[action_primary_key_value] = action

                # Accumulate keys into the action_data schema
                for column_name in action.keys():
                    if column_name not in self._action_column_names:
                        self._action_column_names[column_name] = True
                
        # Handle events
        if self._event_table is not None:
            event_row = OrderedDict()
            namespacified_row = namespacify(dsjson_row['c'])
            
            # Primary key
            if self._event_table.primary_key:
                if isinstance(self._event_table.primary_key_feature, str):
                    event_row[self._event_table.primary_key] = dsjson_row[self._event_table.primary_key_feature]
                else:
                    event_row[self._event_table.primary_key] = namespacified_row[self._event_table.primary_key_feature]
        
            # Toplevel event fields
            _extract_toplevel_fields(dsjson_row, self._event_table.toplevel_dict, event_row)
        
            # Handle the special _label_key column mapping if present and we're mapping actions
            if '_label_key' in self._event_table.toplevel_dict and self._action_table.primary_key:
                action_index = dsjson_row['_label_Action'] - 1
                event_row[self._event_table.toplevel_dict['_label_key']] = actions[action_index][self._action_table.primary_key]
        
            # Context features (including namespaces).  Keys are always tuples of (namespace, feature)
            _extract_features(namespacified_row, self._event_table.column_dict, event_row)
            
            # For standard Contextual Bandits, collect Action keys, probability, and index
            if self._full_transform and 'a' in dsjson_row and 'p' in dsjson_row:
                action_vector = dsjson_row['a']
                probability_vector = dsjson_row['p']
                for rank_index, action_index in enumerate(action_vector):
                    action_primary_key_value = actions[action_index - 1][self._action_table.primary_key]
                    event_link_dict = {
                        'key': action_primary_key_value,
                        'probability': probability_vector[rank_index],
                        'index': action_index
                    }
                
                    if self._changing_actions:
                        event_link_dict['surrogate'] = self._action_data[action_primary_key_value][self._action_surrogate_column]
                    for event_link_feature, value in event_link_dict.items():
                        if event_link_feature in self._event_links_dict:
                            column_name = self._event_links_dict[event_link_feature].replace('*', str(rank_index + 1))
                            event_row[column_name] = value
            
            # Accumulate keys into the event_data schema
            for column_name in event_row.keys():
                if column_name not in self._event_column_names:
                    self._event_column_names[column_name] = True
                    
            self._event_data.append(event_row)

        self._event_row_count += 1
        self._write_progress(self._write_every)

        
    def _extract_actions_from_dsjson(self, json_row):
        '''Given a single dsjson record, extract all of the actions into a list of dicts, where each dict maps column_name-->value.
    
        Parameters:
        json_row -- dsjson record
    
        Returns:
        A list of actions, each of which is a mapping from column_name-->value.
        '''
        result = []
    
        # In DSJson format, both context and action data is stored in a single dictionary called "c".  Within "c",
        # action data is always stored in the "_multi" key.  Everything else in "c" is part of the context.
        for action_dict in json_row['c']['_multi']:
            action_row = OrderedDict()
            namespacified_row = namespacify(action_dict)
            if isinstance(self._action_table.primary_key_feature, str):
                action_row[self._action_table.primary_key] = action_dict[self._action_table.primary_key_feature]
            else:
                action_row[self._action_table.primary_key] = namespacified_row[self._action_table.primary_key_feature]
            _extract_toplevel_fields(action_dict, self._action_table.toplevel_dict, action_row)
            _extract_features(namespacified_row, self._action_table.column_dict, action_row)
            result.append(action_row)
        return result

        
    def _read_file(self, dsjson_file):
        if self._write_every:
            print(f'Reading file {dsjson_file}')
        with gzip.open(dsjson_file, 'rt', encoding='utf8') if dsjson_file.endswith('.gz') else open(dsjson_file, 'r', encoding="utf8") as vw_file:
            for line in vw_file:
                try:
                    dsjson_row = json.loads(line.strip())
                    if 'c' in dsjson_row:
                        self._read_row(dsjson_row)
                    else:
                        self._error_count += 1
                except json.JSONDecodeError:
                    self._error_count += 1
        # Output the total number of rows regardless
        self._write_progress(1)

        
    def _read_files(self, dsjson_files):
        if isinstance(dsjson_files, str):
            self._read_file(dsjson_files)
        else:
            for dsjson_file in dsjson_files:
                self._read_file(dsjson_file)

            
    def _begin_batch(self):
        self._action_data = OrderedDict()
        self._event_data = []
        self._action_data_archive = []

        
    def _create_dense_row(self, column_names, sparse_row):
        '''Convert a row from a sparse format (row represented as a dictionary of column-value pairs) to a dense format
        (row represented as a list of values, with one entry per column).
        
        Parameters:
        column_names -- List / iterable containing the names of all columns.
        sparse_row -- Dict holding the column-value pairs to convert.
        
        Returns:
        A list of values.  Each value in the list goes with the corresponding column name in the column_names list. 
        Missing values will be represented via None.  Any data in the dict with a column name that does not match one of the
        column names in the list will be discarded.
        '''
        dense_row = []
        for key in column_names:
            if key in sparse_row:
                dense_row.append(sparse_row[key])
            else:
                dense_row.append(None)
        return dense_row

    
    def _write_actions(self, actions, csv_actions_writer):
        rows_written = 0
        for action_row in actions:
            csv_actions_writer.writerow(self._create_dense_row(self._action_column_names.keys(), action_row))
            rows_written += 1
        return rows_written

    
    def _get_event_column_names(self):
        column_names = []
        for key in self._event_column_names.keys():
            column_names.append(key)
        return column_names

    
    def _get_action_column_names(self):
        # Remap column names according to action naming scheme and write
        column_names = []
        for key in self._action_column_names.keys():
            column_names.append(key)
        return column_names

    
    def _write_actions_batch(self, actions_file, rows_written=0):
        with open(actions_file, 'w', newline='') as csv_actions_file:
            csv_actions_writer = csv.writer(csv_actions_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            out_rows = rows_written
            
            # Column names (as header row)
            csv_actions_writer.writerow(self._get_action_column_names())
            
            # Archived actions
            out_rows += self._write_actions(self._action_data_archive, csv_actions_writer)
            self._action_data_archive = []
            
            # Current actions
            out_rows += self._write_actions(self._action_data.values(), csv_actions_writer)
        
        if self._write_every:
            print(f'Wrote {out_rows} rows and {len(self._action_column_names.keys())} columns to {actions_file}')

            
    def _write_event_row(self, event_row, csv_events_writer):
        csv_events_writer.writerow(self._create_dense_row(self._event_column_names.keys(), event_row))

        
    def _write_events_batch(self, events_file):
        with open(events_file, 'w', newline='') as csv_events_file:
            csv_events_writer = csv.writer(csv_events_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            out_rows = 0
            
            # Write column names
            csv_events_writer.writerow(self._get_event_column_names())
                    
            # Write data
            for event_row in self._event_data:
                self._write_event_row(event_row, csv_events_writer)
                out_rows += 1
            
            if self._write_every:
                print(f'Wrote {out_rows} rows and {len(self._event_column_names.keys())} columns to {events_file}') 

    
    def _write_batch(self, events_file, actions_file):
        if events_file is not None:
            self._write_events_batch(events_file)
        if actions_file is not None:
            self._write_actions_batch(actions_file)
        self._begin_batch()

        
    def _get_events_dataframe(self):
        if self._event_table is None:
            return None
        column_names = self._get_event_column_names()
        data = []
        for dict_row in self._event_data:
            # Warning: cannot use column_names here becuase those have already had delimiters applied.
            data.append(self._create_dense_row(self._event_column_names.keys(), dict_row))
        dataframe = pd.DataFrame(columns=column_names, data=data)
        if self._event_table.primary_key:
            dataframe.set_index(self._event_table.primary_key, inplace=True)
        return dataframe

    
    def _get_actions_dataframe(self):
        if self._action_table is None:
            return None
        column_names = self._get_action_column_names()
        all_actions = []
        for action in self._action_data_archive:
            # Warning: cannot use column_names here becuase those have already had delimiters applied.
            all_actions.append(self._create_dense_row(self._action_column_names.keys(), action))
        for action in self._action_data.values():
            # Warning: cannot use column_names here becuase those have already had delimiters applied.
            all_actions.append(self._create_dense_row(self._action_column_names.keys(), action))
        dataframe = pd.DataFrame(columns=column_names, data=all_actions)
        if self._changing_actions:
            dataframe.set_index(self._action_surrogate_column, inplace=True)
        else:
            dataframe.set_index(self._action_table.primary_key, inplace=True)
        return dataframe

    
def extract_csv(input_dsjson, config, event_csv=None, action_csv=None, coalesce_nulls=True, output_every=0):
    '''Convenience function to perform an extraction from a DSJson file, placing the results into CSV files.
    May also be used to perform a full conversion, based on the provided config.
        
    Parameters:
    input_dsjson -- dsjson file (or list of dsjson files) from which to extract.
    config -- configuration for controlling the extract.
    event_csv -- (optional) name of output csv file for the event table.
    action_csv -- (optional) name of output csv file for the action table.
    coalesce_nulls -- True to minimize null action features by combining values.
    output_every -- Write progress to stdout after this many rows.  Set to 0 to suppress all output.
    '''
    TabularConverter(config=config, coalesce_nulls=coalesce_nulls, write_every=output_every).convert_to_csv(input_dsjson, event_csv, action_csv)
    
    
def extract_pandas(input_dsjson, config, coalesce_nulls=True, output_every=0):
    '''Convenience function to perform an extraction from a DSJson file, returning the results as one or two
    Pandas DataFrames (depending on config settings).  May also be used to perform a full conversion.
    
    Parameters:
    input_dsjson -- dsjson file (or list of dsjson files) from which to extract.
    config -- configuration for controlling the extract.
    coalesce_nulls -- True to minimize null action features by combining values.
    output_every -- Write progress to stdout after this many rows.  Set to 0 to suppress all output.
    
    Returns:
    events_dataframe -- Pandas dataframe holding Events, if the config specifies an event table only.
    actions_dataframe -- Pandas dataframe holding Actions, if the config specifies an action table only.
    (events_dataframe, actions_dataframe) -- Tuple of Events and Actions dataframes, if the config specifies both tables.
    '''
    return TabularConverter(config=config, coalesce_nulls=coalesce_nulls, write_every=output_every).convert_to_pandas(input_dsjson)
    

def _parse_action_id(action_id_str):
    if action_id_str is None:
        return None
    elif '.' in action_id_str:
        split_str = action_id_str.split('.', 1)
        return (split_str[0], split_str[1])
    else:
        return action_id_str
    
    
def _process_config(config_str):
    if config_str is None:
        return None
    elif config_str.strip().startswith('{'):
        return json.loads(config_str)
    else:
        with open(config_str) as f:
            return json.load(f)
        
        
def main(argv=sys.argv[1:]):
    '''Enable running of the converter on the command-line
    '''
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description="Convert dsjson file(s) into CSV-formatted tabular data")
    parser.add_argument("--input", type=str, required=True, help="Input file or directory.  If a directory, all files inside will be converted.")
    parser.add_argument("--config", type=str, required=False, help="Configuration to control the transformation / extract.  May be a Json file or an inline Json expression")
    parser.add_argument("--actions_dir", type=str, help="Directory for actions file(s)")
    parser.add_argument("--actions_data_file", type=str, default="actions.csv", help="Name of actions data file")
    parser.add_argument("--events_dir", type=str, help="Directory for events file(s)")
    parser.add_argument("--events_data_file", type=str, default="events.csv", help="Name of events data file")
    parser.add_argument("--stream_output", type=bool, default=False, help="True to stream output to support large files")
    parser.add_argument("--actions_schema_file", type=str, default="actions_schema.csv", help="Name of actions schema (column names) file; only used if stream_output is set")
    parser.add_argument("--events_schema_file", type=str, default="events_schema.csv", help="Name of events schema (column names) file; only used if stream_output is set")
    parser.add_argument("--coalesce_nulls", type=bool, default=True, help="True to minimize null actions by combining values")
    parser.add_argument("--write_every", type=int, default=10000, help="Output status information after this many rows")
    
    parser.add_argument("--changing_actions", type=bool, required=False, help="[Deprecated: use config instead] "
                        "True to model changing actions")
    parser.add_argument("--action_id", type=str, required=False, help="[Deprecated: use config instead] "
                        "Action feature to use as an identifier for actions.  May be a single name or a pair of <namespace>.<feature>")
    parser.add_argument("--full_transform", type=bool, required=False, help="[Deprecated: use config instead] "
                        "If True, include enough output in the Events table to enable the dsjson to be reconstructed later")

    args = parser.parse_args(argv)
    
    input_files = []
    if os.path.isdir(args.input):
        for dir_name, _, file_list in os.walk(args.input):
            for file in file_list:
                input_files.append(os.path.join(dir_name, file))
    else:
        input_files.append(args.input)
        
    if args.actions_dir:
        os.makedirs(args.actions_dir, exist_ok=True)
        actions_data_file = os.path.join(args.actions_dir, args.actions_data_file)
    else:
        actions_data_file = args.actions_data_file
        
    if args.events_dir:
        os.makedirs(args.events_dir, exist_ok=True)
        events_data_file = os.path.join(args.events_dir, args.events_data_file)
    else:
        events_data_file = args.events_data_file

    converter = TabularConverter(config=_process_config(args.config), coalesce_nulls=args.coalesce_nulls, write_every=args.write_every,
                                 changing_actions=args.changing_actions, action_id=_parse_action_id(args.action_id), full_transform=args.full_transform)
    if args.stream_output:
        if args.actions_dir:
            actions_schema_file = os.path.join(args.actions_dir, args.actions_schema_file)
        else:
            actions_schema_file = args.actions_schema_file
        if args.events_dir:
            events_schema_file = os.path.join(args.events_dir, args.events_schema_file)
        else:
            events_schema_file = args.events_schema_file
        converter.convert_streaming(input_files, events_data_file, events_schema_file, actions_data_file, actions_schema_file)
    else:
        converter.convert_to_csv(input_files, events_data_file, actions_data_file)


if __name__ == '__main__':
    main()
