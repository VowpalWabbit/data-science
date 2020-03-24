import numpy as np
import pandas as pd
import json
from collections import OrderedDict
from enum import Enum
import csv
import gzip


def _namespacify_dict(result, ns, obj):
    for key, value in obj.items():
        if key.startswith('_'):
            continue
        if type(value) is dict:
            _namespacify_dict(result, key, value)      
        elif type(value) is list:
            _namespacify_list(result, key, value)        
        else:
            result[(ns, key)] = value

            
def _namespacify_list(result, ns, l):
    for value in l:
        if type(value) is dict:
            _namespacify_dict(result, ns, value)
            
        elif type(value) is list:
            _namespacify_list(result, ns, value)
            
        else:
            # Ignore stray values in lists
            pass

        
def _namespacify(json_snippet):
    '''Simplify a snippet of JSON-formatted VW data into a flat dictionary mapping the following:
    (namespace, feature) -> value
    Flattens all nesting and discards all fields starting with underscore.  If there are multiple
    features with the same name defined with the same namespace, prefers the latest one.
    
    Parameters:
    json_snippet -- object representing JSON data.  Can be a JSON object or array.
    
    Returns:
    JSON object with namespaces simplified.
    '''
    result = OrderedDict()
    if type(json_snippet) is list:
        _namespacify_list(result, '', json_snippet)
    elif type(json_snippet) is dict:
        _namespacify_dict(result, '', json_snippet)
    return result


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


class TabularConverter:
    '''TabularConverter is a class used to convert data from dsjson format into a standard tabular format of rows and columns,
    with options to generate CSV files and Pandas Dataframes.  More specifically, a single dsjson dataset will be converted into
    two tables: an Events table and an Actions table.
    
    TabularConverter generally works on DSJson-formatted data, although it does make a few assumptions:
    * Actions all appear in a single '_multi' list that appears under the toplevel 'c' element.
    * Every action has a unique identifier.  The identifier appears in a '_tag' attribute on that action.
    * Each dsjson row defines 'EventId', 'Timestamp', and 'Version'.
    * All label fields are defined on each dsjson row ('_label_Action', '_label_cost', '_label_probability')
    
    TabularConverter is fully compatible with data produced by the Azure Personalizer Service, which follows all of the assumptions above.
    
    The key used to join the Actions table back to the Events table depends on how you've set changing_actions as follows:
    If changing_actions is false, the tables will be joined using the Action's unique identifier.
    If changing_actions is true, the converter will generate a new (surrogate) key to represent each Action version and the tables will be joined on this key.
    '''
    def __init__(self, changing_actions=False, coalesce_nulls=True, denormalize_label=False, action_id='_tag'):
        '''Create a new TabularConverter instance.
        Parameters:
        changing_actions -- (default False).  True to create a new Action row every time any action features change.  False to overwrite action features
                            if they change.  Also, setting this to True will create a Key column in the generated Action table along with corresponding
                            Action.rank._key columns in the Event table.
        coalesce_nulls -- (default True).  True to overwrite Null values in Action rows, even if changing_actions=True.  False to treat Nulls as a distinct
                          action feature value, even if this forces creation of a new Action row.
        denormalize_label -- (default False).  True to record all of the features of the label (chosen action) directly in the Events table.
        action_id -- (default '_tag').  Action feature to use as an identifier.  The default is '_tag'.  May be either '_tag', a single feature, or a tuple of (namespace,feature).
                     If action_id is a single feature then the default namespace will be used.
        '''        
        self._write_every = 10000
        self._action_delimiter = '.'
        self._event_delimiter = '.'
        self._changing_actions = changing_actions
        self._coalesce_nulls = coalesce_nulls
        self._denormalize_label = denormalize_label
        self._action_id = action_id
        self._action_key = 0
        self._reset()
    
    
    def convert_to_csv(self, input_files, events_file, actions_file):
        '''Simple conversion.  This converter reads multiple input files and generates a single CSV each
        for events and actions.  It buffers all data in memory before writing, and so will not scale to
        huge files.
        
        Parameters:
        input_files -- list of input files to convert
        events_file -- output filename for events file (in csv format)
        actions_file -- output filename for actions file (in csv format)
        '''
        self._reset()
        self._read_files(input_files)
        self._write_batch(events_file, actions_file)


    def convert_to_pandas(self, input_files):
        '''Converter to Pandas dataframes.  This converter reads multiple input files and returns two
        Pandas dataframes, one with Events and one with Actions.  The Events dataframe will be
        indexed by EventID.  The Actions dataframe will be indexed by Key (if modeling changing actions)
        or Tag (if not).
        
        Parameters:
        input_files -- list of input files to convert
        
        Returns a tuple of:
        events_dataframe -- Pandas dataframe holding Events
        actions_dataframe -- Pandas dataframe holding Actions
        '''
        self._reset()
        self._read_files(input_files)
        events_dataframe = self._get_events_dataframe()
        actions_dataframe = self._get_actions_dataframe()
        return (events_dataframe, actions_dataframe)

    
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
                                    self._write_event_row(self.event_data[0], csv_events_writer)
                                    event_rows += 1
                                    self.event_data = []
                                
                                    # Write actions in the archive and dump the archive
                                    action_rows += self._write_actions(self.action_data_archive, csv_actions_writer)
                                    self.action_data_archive = []
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
            action_rows += self._write_actions(self.action_data_archive, csv_actions_writer)
            action_rows += self._write_actions(self.action_data.values(), csv_actions_writer)
        
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
        event_row = {}
        
        # Handle actions
        actions = self._extract_actions_from_dsjson(dsjson_row)
        for action in actions:
            tag = action['_tag']
            if tag not in self.action_data:
                # Brand new action
                if self._changing_actions:
                    self._action_key += 1
                    action['_key'] = self._action_key
                    action['_timestamp'] = dsjson_row['Timestamp']
                    action['_latest'] = 1
                self.action_data[tag] = action
            else:
                old_action = self.action_data[tag]
                if self._changing_actions:
                    if _actions_differ(old_action, action, self._coalesce_nulls):
                        # Existing action, but some values changed; generate a new action row
                        old_action['_latest'] = 0
                        self._action_key += 1
                        action['_key'] = self._action_key
                        action['_timestamp'] = dsjson_row['Timestamp']
                        action['_latest'] = 1
                        
                        # Move the old action into the archive and replace with the new action
                        self.action_data_archive.append(old_action)
                        self.action_data[tag] = action
                else:
                    # If not modeling changing actions, replace and log any changed action features in-place
                    conflicts = _coalesce_actions(old_action, action)
                    for (key, old_value, new_value) in conflicts:
                        if self._write_every and self._conflict_count < 100:
                            print(f'Conflict! Tag {tag}.  Feature {key}.  Expected {old_value} found {new_value}')
                        self._conflict_count += 1
                    
            # Accumulate keys into the action_data schema
            for column_name in action.keys():
                if column_name not in self._action_column_names:
                    self._action_column_names[column_name] = True

        # Event identifying fields
        event_row['EventId'] = dsjson_row['EventId']
        event_row['Timestamp'] = dsjson_row['Timestamp']
        event_row['Version'] = dsjson_row['Version']
        
        # Context features (including namespaces).  Keys are always tuples of (namespace, feature)
        for context_key, context_val in _namespacify(dsjson_row['c']).items():
            (namespace, feature) = context_key
            event_row[('Context', namespace, feature)] = context_val
            
        # Label (Chosen action).  Optionally denormalize all label features.
        action_index = dsjson_row['_label_Action'] - 1
        event_row['Label'] = actions[action_index]['_tag']
        event_row[('Label','_p')] = dsjson_row['_label_probability']
        event_row[('Label','_cost')] = dsjson_row['_label_cost']
        if self._denormalize_label:
            for action_key, action_val in _namespacify(dsjson_row['c']['_multi'][action_index]).items():
                event_row[('Label', action_key[0], action_key[1])] = action_val
            
        # Action keys, probability, and index
        action_vector = dsjson_row['a']
        probability_vector = dsjson_row['p']
        for rank_index, action_index in enumerate(action_vector):
            action_tag = actions[action_index - 1]['_tag']
            event_row[f'ActionRank.{rank_index + 1}'] = action_tag
            event_row[(f'ActionRank.{rank_index + 1}','_p')] = probability_vector[rank_index]
            event_row[(f'ActionRank.{rank_index + 1}','_index')] = action_index
            if self._changing_actions:
                event_row[(f'ActionRank.{rank_index + 1}','_key')] = self.action_data[action_tag]['_key']
            
        # Accumulate keys into the event_data schema
        for column_name in event_row.keys():
            if column_name not in self._event_column_names:
                self._event_column_names[column_name] = True
                    
        self.event_data.append(event_row)
        self._event_row_count += 1
        self._write_progress(self._write_every)

        
    def _extract_actions_from_dsjson(self, json_row):
        '''Given a single dsjson record, extract all of the actions into a list, keeping the actions in dsjson format
        but with namespaces simplified to a single level
    
        Parameters:
        json_row -- dsjson record
    
        Returns:
        A list of actions, each of which is a snippet of dsjson
        '''
        result = []
    
        # In DSJson format, both context and action data is stored in a single dictionary called "c".  Within "c",
        # action data is always stored in the "_multi" key.  Everything else in "c" is part of the context.
        for action_dict in json_row['c']['_multi']:
            action_row = _namespacify(action_dict)

            # Find the action's ID and set it as a special '_tag' attribute
            if self._action_id == '_tag':
                action_row['_tag'] = action_dict['_tag']
            elif isinstance(self._action_id, str):
                action_row['_tag'] = action_row[('_',self._action_id)]
            else:
                action_row['_tag'] = action_row[self._action_id]
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
        for dsjson_file in dsjson_files:
            self._read_file(dsjson_file)

            
    def _begin_batch(self):
        self.action_data = OrderedDict()
        self.event_data = []
        self.action_data_archive = []

        
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
            if isinstance(key, str):
                column_names.append(key)
            else:
                column_names.append(self._event_delimiter.join(key))
        return column_names

    
    def _get_action_column_names(self):
        # Remap column names according to action naming scheme and write
        column_names = []
        for key in self._action_column_names.keys():
            if key == '_tag':
                column_names.append('Tag')
            elif key == '_key':
                column_names.append('Key')
            elif key == '_latest':
                column_names.append('Latest')
            elif key == '_timestamp':
                column_names.append('Timestamp')
            else:
                column_names.append(self._action_delimiter.join(('Action', key[0], key[1])))
        return column_names

    
    def _write_actions_batch(self, actions_file, rows_written=0):
        with open(actions_file, 'w', newline='') as csv_actions_file:
            csv_actions_writer = csv.writer(csv_actions_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            out_rows = rows_written
            
            # Column names (as header row)
            csv_actions_writer.writerow(self._get_action_column_names())
            
            # Archived actions
            out_rows += self._write_actions(self.action_data_archive, csv_actions_writer)
            self.action_data_archive = []
            
            # Current actions
            out_rows += self._write_actions(self.action_data.values(), csv_actions_writer)
        
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
            for event_row in self.event_data:
                self._write_event_row(event_row, csv_events_writer)
                out_rows += 1
            
            if self._write_every:
                print(f'Wrote {out_rows} rows and {len(self._event_column_names.keys())} columns to {events_file}') 

    
    def _write_batch(self, events_file, actions_file):
        self._write_events_batch(events_file)
        self._write_actions_batch(actions_file)
        self._begin_batch()

        
    def _get_events_dataframe(self):
        column_names = self._get_event_column_names()
        data = []
        for dict_row in self.event_data:
            # Warning: cannot use column_names here becuase those have already had delimiters applied.
            data.append(self._create_dense_row(self._event_column_names.keys(), dict_row))
        dataframe = pd.DataFrame(columns=column_names, data=data)
        dataframe.set_index('EventId', inplace=True)
        return dataframe

    
    def _get_actions_dataframe(self):
        column_names = self._get_action_column_names()
        all_actions = []
        for action in self.action_data_archive:
            # Warning: cannot use column_names here becuase those have already had delimiters applied.
            all_actions.append(self.create_dense_row(self._action_column_names.keys(), action))
        for action in self.action_data.values():
            # Warning: cannot use column_names here becuase those have already had delimiters applied.
            all_actions.append(self._create_dense_row(self._action_column_names.keys(), action))
        dataframe = pd.DataFrame(columns=column_names, data=all_actions)
        if self._changing_actions:
            dataframe.set_index('Key', inplace=True)
        else:
            dataframe.set_index('Tag', inplace=True)
        return dataframe
        

def _parse_action_id(action_id_str):
    if '.' in action_id_str:
        split_str = action_id_str.split('.', 1)
        return (split_str[0], split_str[1])
    else:
        return action_id_str
        
        
def main():
    '''Enable running of the converter on the command-line
    '''
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description="Convert dsjson file(s) into CSV-formatted tabular data")
    parser.add_argument("--input", type=str, required=True, help="Input file or directory.  If a directory, all files inside will be converted.")
    parser.add_argument("--actions_dir", type=str, help="Directory for actions file(s)")
    parser.add_argument("--actions_data_file", type=str, default="actions.csv", help="Name of actions data file")
    parser.add_argument("--events_dir", type=str, help="Directory for events file(s)")
    parser.add_argument("--events_data_file", type=str, default="events.csv", help="Name of events data file")
    parser.add_argument("--changing_actions", type=bool, default=False, help="True to model changing actions")
    parser.add_argument("--coalesce_nulls", type=bool, default=True, help="True to minimize null actions by combining values")
    parser.add_argument("--action_id", type=str, default="_tag", help="Action feature to use as an identifier for actions.  May be a single name or a pair of <namespace>.<feature>")
    parser.add_argument("--stream_output", type=bool, default=False, help="True to stream output to support large files")
    parser.add_argument("--actions_schema_file", type=str, default="actions_schema.csv", help="Name of actions schema (column names) file; only used if stream_output is set")
    parser.add_argument("--events_schema_file", type=str, default="events_schema.csv", help="Name of events schema (column names) file; only used if stream_output is set")

    args = parser.parse_args()
    
    input_files = []
    if os.path.isdir(args.input):
        for dir_name, subdir_list, file_list in os.walk(args.input):
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

    converter = TabularConverter(changing_actions=args.changing_actions, coalesce_nulls=args.coalesce_nulls, action_id=_parse_action_id(action_id))
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
        converter.convert(input_files, events_data_file, actions_data_file)


if __name__ == '__main__':
    main()
