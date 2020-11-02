import csv
from collections import OrderedDict
import json
import pandas as pd
import numpy as np
import gzip

from .converter_common import default_patch_config, patch_only_config, default_convert_config
from .converter_common import DEFAULT_NAMESPACE
from .converter_common import jsonify, merge_dicts, namespacify
from .converter_common import process_config_columns, process_event_links, get_match
from .converter_common import ConfigError


class PatcherTable:
    '''PatcherTable holds the attributes and data of a table used for patching.  E.g. Event Table and Action Table.
    '''
    
    def __init__(self, table_config, table_df):
        '''Create an instance of PatcherTable.
        
        Parameters:
        table_config -- configuration section for this table.  E.g. the "event_table" or "action_table" section.
        table_df -- Pandas dataframe containing at least the column definitions for this table.
        '''
        self.table_df = table_df
        self.primary_key = None
        self.primary_key_feature = None
        self.patch_mapping = None
        self.add_mapping = None
        self.toplevel_mapping = None
        
        patch_config = table_config['patch'] if 'patch' in table_config else {}
        columns_config = table_config['columns'] if 'columns' in table_config else {}
        self.primary_key = table_config['primary_key'] if 'primary_key' in table_config else None
        if self.primary_key == self.table_df.index.name:
            pass
        elif self.primary_key not in self.table_df.columns:
            raise ConfigError(config=table_config, 
                              message=f'primary key "{self.primary_key}" is not set to either '
                                      f'the index ({table_df.index.name}) '
                                      f'or as a column in the Table ({self.table_df.columns})')
        else:
            self.table_df = self.table_df.set_index(self.primary_key)

        (patch_toplevel_mapping, self.patch_mapping, patch_primary_key_feature) = \
            DSJsonPatcher._get_column_mappings(patch_config, list(self.table_df.columns), self.primary_key)
        (add_toplevel_mapping, self.add_mapping, add_primary_key_feature) = \
            DSJsonPatcher._get_column_mappings(columns_config, list(self.table_df.columns), self.primary_key)
        self.patch_mapping = merge_dicts(self.patch_mapping, self.add_mapping)
        self.toplevel_mapping = merge_dicts(patch_toplevel_mapping, add_toplevel_mapping)
                
        # Get the column mapping for the primary key (if defined).
        if self.primary_key:
            self.primary_key_feature = add_primary_key_feature if add_primary_key_feature else patch_primary_key_feature
            if not self.primary_key_feature:
                self.primary_key_feature = self.primary_key


class DSJsonPatcher:
    def __init__(self, config, event_df=None, action_df=None):
        '''Class for patching values in a dsjson file, using event and/or action tables to provide the values.
        
        Parameters:
        config -- configuration object defining how to perform the patch operation
        event_df -- Event Table (optional), represented as a Pandas DataFrame.  Does not need to contain the full Event Table
                    if you pass rows directly to the patch_row method. 
        action_df -- Action Table (optional), represented as a Pandas DataFrame.  Must contain the full Action Table.
        '''
        self._event_df = event_df
        self._event_table = None
        self._action_df = action_df
        self._action_table = None
        self._action_link_mappings = None
        self._actions = None
        
        event_config = config['event_table'] if 'event_table' in config else None
        action_config = config['action_table'] if 'action_table' in config else None

        # Event table config
        if self._event_df is not None:
            self._event_table = PatcherTable(event_config, event_df)
            if self._event_table.primary_key is None:
                raise ConfigError(config=event_config, message=f'primary_key must be defined in event_table')
            self._event_df = self._event_table.table_df
        
        # Action table config
        if self._action_df is not None:
            self._action_table = PatcherTable(action_config, action_df)
            if self._action_table.primary_key is None:
                raise ConfigError(config=action_config, message=f'primary_key must be defined in action_table')
            self._action_df = self._action_table.table_df

        # Event to action links
        if event_config and 'links' in event_config and self._event_df is not None:
            self._action_link_mappings = DSJsonPatcher._get_link_mappings(event_config['links'], self._event_df.columns)


    @staticmethod
    def _get_column_mappings(column_config, data_columns, primary_key=None):
        '''Helper function to construct column mappings from configuration and from the list of columns in a dataframe.
        May be used on either the Event Table or the Action Table.
        
        Parameters:
        column_config -- the section of the configuration object defining the column mappings to process.
        data_columns -- list of column names from the data to be mapped.
        primary_key -- (optional) name of the primary key column, if any.
        
        Returns:
        A tuple of (toplevel_mapping, column_mapping, primary_key_feature), where:
        toplevel_mapping -- mapping of toplevel columns, as a dict mapping the JSON field name of the toplevel column to 
                            a pair of (column_index, column_name).
        column_mapping -- mapping of feature columns, as a dict mapping a tuple of (namespace, feature) to 
                          a pair of (column_index, column_name).
        primary_key_feature -- toplevel element (string) or feature (tuple of (namespace, feature)) mapped to the primary key.
                               None if no primary key mapping is defined.
        '''
        toplevel_dict, column_dict, primary_key_feature = process_config_columns(column_config, primary_key)
        toplevel_mapping = {}
        column_mapping = {}
        
        # Toplevel mappings
        for attribute_name, column_name in toplevel_dict.items():
            if column_name == primary_key:
                primary_key_feature = attribute_name
            elif column_name in data_columns:
                toplevel_mapping[attribute_name] = (data_columns.index(column_name), column_name)
            else:
                raise ConfigError(config=column_config, message=f'Toplevel mapping "{column_name}" is not defined as a column or index')
        
        for feature_key, column_exprs in column_dict.items():
            # Full wildcard mappings.
            if feature_key == ():
                for (namespace_wildcard, feature_wildcard, column_wildcard) in column_exprs:
                    for column_index, column_name in enumerate(data_columns):
                        namespace_and_feature = get_match(column_name, column_wildcard.replace('**', '*'))
                        if namespace_and_feature is None:
                            continue
                        separator = column_wildcard[-3]
                        separator_pos = namespace_and_feature.find(separator)
                        if separator_pos < 0:
                            continue
                        namespace_match = namespace_and_feature[:separator_pos]
                        namespace = namespace_wildcard.replace('*', namespace_match)
                        feature_match = namespace_and_feature[separator_pos+1:]
                        feature = feature_wildcard.replace('*', feature_match)
                        column_mapping[(namespace,feature)] = (column_index, column_name)

            # Single wildcard mappings.
            elif isinstance(feature_key, str):
                namespace = feature_key
                for feature_wildcard, column_wildcard in column_exprs:
                    for column_index, column_name in enumerate(data_columns):
                        match = get_match(column_name, column_wildcard)
                        if match is None:
                            continue
                        feature = feature_wildcard.replace('*', match)
                        column_mapping[(namespace, feature)] = (column_index, column_name)
                            
            # Single column mapping.
            else:
                namespace, feature = feature_key
                for column_name in column_exprs:
                    if column_name == primary_key:
                        primary_key_feature = (namespace, feature)
                    elif column_name not in data_columns:
                        raise ConfigError(config=column_config, message=f'Feature mapping "{column_name}" is not defined as a column or the primary key')
                    else:
                        column_mapping[(namespace, feature)] = (data_columns.index(column_name), column_name)
        
        return toplevel_mapping, column_mapping, primary_key_feature
    
    
    @staticmethod
    def _get_link_mappings(links_config, columns):
        '''Helper function to process link configurations, which can be a bit complex.  This is the
        links: { } section of the configuration that can be specified inside the event table when doing a full
        transform.
        
        Parameters:
        links_config -- configuration element for links.  Must be a dict.
        columns -- list of column names from the event table.
        
        Returns:
        A triple of (scheme, link_mapping, mapped_fields):
        scheme -- identifies the naming scheme for link columns.  Right now we just support 'rank' which means that the
                  link identifier will be the 1-based rank for how VW selected the action (1 == top action, 2 == second action, etc)
        link_mapping -- a dict mapping the link identifier (e.g. rank) to the mapping (e.g. 'key', 'index', 'probability') to
                        a pair (column_index, column_name).
        mapped_fields -- a set listing all of the fields (from 'key', 'index', and 'probability') that got mapped.  'key' must always
                         be mapped but the other two are optional.
        '''
        allowed_schemes = ['rank']
        allowed_mappings = ['key', 'index', 'probability']

        link_mapping = {}
        if not isinstance(links_config, dict):
            raise ConfigError(config=links_config, message=f'links configuration must be a dict')
        scheme = links_config['scheme'] if 'scheme' in links_config else 'rank'
        if scheme not in allowed_schemes:
            raise ConfigError(config=links_config, message=f'illegal scheme \'{scheme}\'.  Must be one of {allowed_schemes}')

        if not 'columns' in links_config:
            raise ConfigError(config=links_config, message=f'links configuration must contain a \'columns\' declaration')
        if not isinstance(links_config['columns'], dict):
            raise ConfigError(config=links_config, message=f'links configuration for columns must be a dict')

        mapped_fields = set()
        for key, value in links_config['columns'].items():
            if not '*' in key or len(key) < 2:
                raise ConfigError(config=links_config, message=f'links column configuration \'{key}\' is illegal; '
                                  f'must be a wildcard')
            if not value in allowed_mappings:
                raise ConfigError(config=links_config, message=f'links column configuration \'{key}\' is illegal; '
                                  f'must map to one of {allowed_mappings}')
            mapped_fields.add(value)
            wildcard_pos = key.find('*')
            first_part = key[0:wildcard_pos]
            last_part = key[wildcard_pos+1:]
            for column_index, column_name in enumerate(columns):
                if column_name.startswith(first_part) and column_name.endswith(last_part):
                    end_pos = column_name.find(last_part)
                    if end_pos > 0:
                        wildcard_value = column_name[wildcard_pos:end_pos]
                    else:
                        wildcard_value = column_name[wildcard_pos:]
                    try:
                        wildcard_value = int(wildcard_value)
                    except ValueError:
                        continue
                    if wildcard_value not in link_mapping:
                        link_mapping[wildcard_value] = {}
                    link_mapping[wildcard_value][value] = (column_index, column_name)

        if 'key' not in mapped_fields:
            raise ConfigError(config=links_config, message='links configuration must include a mapping to \'key\'')
        return (scheme, link_mapping, mapped_fields)
    
    
    @staticmethod
    def _resolve_link_mappings(link_mapping_triple, data_row):
        '''Helper to resolve link mappings given a row of data.
        
        Parameters:
        links_triple --  A triple of (scheme, link_mapping, mapped_fields) as returned from _get_link_mappings
        
        Returns:
        A dict mapping action keys to a dict containing up to four properties:
        key: the action's key
        index: the 1-based position of this action in a dsjson file (will be equal to rank if not specified in the config)
        rank: the 1-based rank of this action as assigned by VW
        probability: the probability assigned to this action by VW (optional)
        
        '''
        (scheme, link_mapping, _) = link_mapping_triple
        actions_by_key = {}
        for link_identifier, link_fields in link_mapping.items():
            linked_action = {}
            linked_action[scheme] = link_identifier
            for link_field, (column_index, _) in link_fields.items():
                linked_action[link_field] = data_row.iat[column_index]
            if linked_action['index'] is None:
                linked_action['index'] = linked_action['rank']
            actions_by_key[linked_action['key']] = linked_action
        return actions_by_key
    
    
    @staticmethod
    def _access_first_dict(json_element):
        '''Helper: access the first dict in which might be a series of nested lists.  Return the first dict if we find it
        (which might be json_element itself), create the dict and return it if we hit an empty list, or return None if
        we hit a scalar.
        
        Parameters: 
        json_element -- element of Json data to search.
        
        Returns:
        A dict (if found) or None (if not found).
        '''
        while isinstance(json_element, list):
            if len(json_element) == 0:
                json_element.append(OrderedDict())
            json_element = json_element[-1]
            
        return json_element if isinstance(json_element, dict) else None


    def _patch_dict(self, namespace, json_dict, data, row_index, column_mapping, mapped_columns):
        '''Helper function to patch values within a JSON dict.  Includes special logic to handle the
        special _multi field that the DSJSon format uses to manage actions.
        
        Parameters:
        namespace -- Name of the current namespace, or DEFAULT_NAMESPACE if this is the default namespace.
        json_dict -- Dict to patch.  Patching will be done in-place.
        data -- Pandas dataframe from which to patch.
        row_index -- Integer index of row in data for patching.
        column_mapping -- Mapping from tuples of (namespace, feature) to column names in the data_row.
        mapped_columns -- Set tracking columns which have been used to patch values.
        '''
        for key,value in json_dict.items():
            # Handle the '_multi' key specially if it shows up at the toplevel
            if key == '_multi' and namespace == DEFAULT_NAMESPACE:
                self._patch_multi(value)
            
            # Pass other "special" keys through unchanged
            elif key.startswith(DEFAULT_NAMESPACE):
                pass
                
            else:
                # Recurse if a dict
                if isinstance(value, dict):
                    self._patch_dict(key, value, data, row_index, column_mapping, mapped_columns)
                    
                # Or a list
                elif isinstance(value, list):
                    self._patch_list(key, value, data, row_index, column_mapping, mapped_columns)
                    
                # Matched a column mapping?
                elif (namespace,key) in column_mapping:
                    (column_index, column_name) = column_mapping[(namespace,key)]
                    mapped_columns.add(column_name)
                    output_value = data.iat[row_index, column_index]
                    if output_value is not None:
                        json_dict[key] = jsonify(output_value)
                    
                    
    def _patch_list(self, namespace, json_list, data, row_index, column_mapping, mapped_columns):
        '''Helper function to patch values within a JSON list.

        Parameters:
        namespace -- Name of the current namespace, or DEFAULT_NAMESPACE if this is the default namespace.
        json_list -- List to patch.  The list will be patched in-place.
        data -- Pandas dataframe from which to patch.
        row_index -- Integer index of row in data for patching.
        column_mapping -- Mapping from tuples of (namespace, feature) to column names in the data_row.
        mapped_columns -- Set tracking columns which have been used to patch values.
        '''
        for i in range(len(json_list)):
            value = json_list[i]
            if isinstance(value, dict):
                self._patch_dict(namespace, value, data, row_index, column_mapping, mapped_columns)
            elif isinstance(value, list):
                self._patch_list(namespace, value, data, row_index, column_mapping, mapped_columns)
            json_list[i] = value
    
    
    def _patch_multi(self, multi_list):
        '''Helper function to patch the special _multi list in a DSJson document.  The _multi list is embedded inside
        the toplevel context namespace dict ('c'), although it actually contains action features rather than event
        features.
        
        Parameters:
        multi_list -- _multi list from the DSJson document.  Will be patched in-place.
        '''
        for action_dict in multi_list:
            # Find the row of Action table data corresponding to this action.  If we can't then don't patch.
            # Note that if the primary key is a (namespace, feature) pair, then we need to simplify namespaces before
            # looking it up.
            if self._action_df is None:
                continue
            if isinstance(self._action_table.primary_key_feature, str):
                if self._action_table.primary_key_feature not in action_dict:
                    continue
                action_primary_key_value = action_dict[self._action_table.primary_key_feature]
            else:
                namespacified_action = namespacify(action_dict)
                if self._action_table.primary_key_feature not in namespacified_action:
                    continue
                action_primary_key_value = namespacified_action[self._action_table.primary_key_feature]
            try:
                action_index = self._action_df.index.get_loc(action_primary_key_value)
            except KeyError:
                continue
                
            # Map columns in the Action table
            mapped_action_columns = set()
            for key,value in action_dict.items():
                if key in self._action_table.toplevel_mapping:
                    (column_index, column_name) = self._action_table.toplevel_mapping[key]
                    mapped_action_columns.add(column_name)
                    action_dict[key] = jsonify(self._action_df.iat[action_index, column_index])
                        
                elif isinstance(value, list):
                    self._patch_list(key, value, self._action_df, action_index, self._action_table.patch_mapping, mapped_action_columns)
                    
                elif isinstance(value, dict):
                    self._patch_dict(key, value, self._action_df, action_index, self._action_table.patch_mapping, mapped_action_columns)
                    
                elif (DEFAULT_NAMESPACE,key) in self._action_table.patch_mapping:
                    (column_index, column_name) = self._action_table.add_mapping[(DEFAULT_NAMESPACE,key)]
                    mapped_action_columns.add(column_name)
                    output_value = self._action_df.iat[action_index, column_index]
                    if output_value is not None:
                        action_dict[key] = jsonify(output_value)
                    
            # Add new action features by applying any unused event mappings
            for field,(column_index,column_name) in self._action_table.toplevel_mapping.items():
                value = self._action_df.iat[action_index, column_index]
                if column_name not in mapped_action_columns and value:
                    action_dict[field] = jsonify(value)
            for (namespace,feature),(column_index,column_name) in self._action_table.add_mapping.items():
                if column_name in mapped_action_columns:
                    continue
                value = self._action_df.iat[action_index, column_index]
                if not value:
                    continue
                if namespace not in action_dict and namespace != DEFAULT_NAMESPACE:
                    action_dict[namespace] = OrderedDict()
                if namespace == DEFAULT_NAMESPACE:
                    action_dict[feature] = jsonify(value)
                else:
                    # Special case: if the namespace already exists but points to a list, find (or add) a dict.
                    namespace_dict = self._access_first_dict(action_dict[namespace])
                    if namespace_dict  is not None:
                        namespace_dict[feature] = jsonify(value)

    
    def _get_label_action(self, event_row, actions_by_key):
        '''This helper function computes the index of the label (aka chosen action) from one of three possible columns
        in the event_row (depending on how column mappings are configured).  The possible columns are (in priority order):
        1. _label_key.  Identifies the label by its key.
        2. _label_Action.  Identifies the label by a 1-based index.
        3. _label_index.  Identifies the label by a 0-based index.
        
        Parameters:
        event_row -- Pandas Series containing the event.
        actions_by_key -- Action links, mapped by key.
        
        Returns:
        The 1-based index on the label, or None if no label was specified or the label was specified by a key that isn't an action
        in this row.
        '''
        if '_label_key' in self._event_table.toplevel_mapping:
            label_key = event_row.iat[self._event_table.toplevel_mapping['_label_key'][0]]
            if label_key in actions_by_key:
                return actions_by_key[label_key]['index']
            
        if '_label_Action' in self._event_table.toplevel_mapping:
            return event_row.iat[self._event_table.toplevel_mapping['_label_Action'][0]]
        
        if '_labelindex' in self._event_table.toplevel_mapping:
            return event_row.iat[self._event_table.toplevel_mapping['_labelindex'][0] + 1]
        
        return None
    
    
    def patch_row(self, input_json, event_data=None, event_index=None):
        '''This is the main public method used to patch an individual row of DSJson data.  It takes a row of DSJson data, uses the
        Event Table, Action Table, and config provided to the class constructor to determine how to patch individual values, and
        returns the patched row.  Unpatched fields are left as-is, in the exact order and structure they were provided.
        Output data types for patched values depend on the data types in the provided tables.  Numeric data (ints, floats) will be
        output as numeric (not quoted).  String data (everything else) will be output as quoted strings.
        
        If no Event Table was provided to the class constructor then all event rows will be used and no event patching will occur.
        Likewise if no Action Table was provided than no action patching will occur.
        
        If an Event Table was provided and there is no row in the Event Table corresponding to the input_json, 
        then the row will be dropped (returns None).
        (except if event_row is specified as below).
        
        If an Action Table was provided and there is no row in the Action Table corresponding to a given action, then the action will
        not be patched.  (This is because actions cannot simply be dropped from the data).
        
        If an event_row is provided, event data will be looked up from event_row, rather than from the Event Table provided to
        the class constructor.  This is used to perform a streaming patch operation without having to load the entire Event Table
        into memory.
        
        Parameters:
        input_json -- input row of data, as a dict in DSJson format.  This dict will be patched in-place.
        event_data -- optional Pandas DataFrame containing a portion of the event_data.  Used for streaming patches.
        event_index -- optional row index for event_data.  If provided, then always use this row of event data to perform the patch.
                       If left as None, lookup the event out of event_data using the event primary key.
        
        Returns:
        True if the event should be retained (either matches a row in the event table or there was no event table).  False if the event
        should be discarded.
        '''
        if event_data is None:
            event_data = self._event_df
        
        # If event_index isn't specified, try to look it up in the Event Table using this row's primary key.
        # If the Event Table was specified and we still can't find an Event Row, just drop this line of input.
        if event_data is not None:
            if isinstance(self._event_table.primary_key_feature, str):
                event_primary_key_value = input_json[self._event_table.primary_key_feature]
            else:
                namespacified_action = namespacify(input_json['c'])
                event_primary_key_value = namespacified_action[self._event_table.primary_key_feature]
            if event_index is None:
                try:
                    # If we match consecutive duplicate rows, take the first.
                    # If we match non-consecutive duplicate rows, ignore the row.
                    event_index = event_data.index.get_loc(event_primary_key_value)
                    if isinstance(event_index, int):
                        pass
                    elif isinstance(event_index, slice):
                        event_index = event_index.start
                    else:
                        return False
                except KeyError:
                    return False
        
        mapped_event_columns = set()
        
        # Map columns in the Event Table
        patch_mapping = self._event_table.patch_mapping if self._event_table is not None else {}
        for key,value in input_json.items():
            # Toplevel columns.  Note that we don't ever patch the primary key.
            if event_data is not None and key in self._event_table.toplevel_mapping:
                (column_index, column_name) = self._event_table.toplevel_mapping[key]
                input_json[key] = jsonify(event_data.iat[event_index, column_index])
                        
            elif key == 'c':
                self._patch_dict(DEFAULT_NAMESPACE, value, event_data, event_index, patch_mapping, mapped_event_columns)

        # Add new event features by applying any unused event mappings
        if event_data is not None:
            for field,(column_index,column_name) in self._event_table.toplevel_mapping.items():
                value = event_data.iat[event_index, column_index]
                if column_name not in mapped_event_columns and value:
                    input_json[field] = jsonify(value)
            for (namespace,feature),(column_index,column_name) in self._event_table.add_mapping.items():
                if column_name in mapped_event_columns:
                    continue
                value = event_data.iat[event_index, column_index]
                if not value:
                    continue
                if namespace not in input_json['c'] and namespace != DEFAULT_NAMESPACE:
                    input_json['c'][namespace] = OrderedDict()
                if namespace == DEFAULT_NAMESPACE:
                    input_json['c'][feature] = jsonify(value)
                else:
                    # Special case: if the namespace already exists but points to a list, iterate through until we encounter a dict
                    namespace_dict = self._access_first_dict(input_json['c'][namespace])
                    if namespace_dict is not None:
                        namespace_dict[feature] = jsonify(value)
        return True
    
    
    def _convert_action(self, action_key, action_row):
        '''Convert a row of action data to DSJson
        
        Parameters:
            action_key: Value of the primary key for this action.
            action_row: Row of data, as a Pandas Series.
        
        Returns:
            the action row, in DSJson.
        '''
        output_json = OrderedDict()
        
        # Action key
        if isinstance(self._action_table.primary_key_feature, str):
            output_json[self._action_table.primary_key_feature] = action_key
        else:
            (namespace, feature) = self._action_table.primary_key_feature
            if namespace not in output_json:
                output_json[namespace] = OrderedDict()
            output_json[namespace][feature] = action_key
        
        # Process toplevel Action mappings (other than the primary key)
        for field,(column_index,column_name) in self._action_table.toplevel_mapping.items():
            if column_name == self._action_table.primary_key:
                continue
            output_json[field] = jsonify(action_row.iat[column_index])
                
        # Process namespace Action mappings
        for (namespace,feature),(column_index,column_name) in self._action_table.patch_mapping.items():
            if namespace not in output_json and namespace != DEFAULT_NAMESPACE:
                output_json[namespace] = OrderedDict()
            if namespace == DEFAULT_NAMESPACE:
                output_json[feature] = jsonify(action_row.iat[column_index]) 
            output_json[namespace][feature] = jsonify(action_row.iat[column_index])
            
        return output_json

    
    def _convert_actions(self):
        '''Convert the action table (represented as a Pandas DataFrame) to dsjson.

        Returns:
            a mapping from action primary key to DSJson snippet for each action.
        '''
        actions = OrderedDict()
        for index, row in self._action_df.iterrows():
            json_snippet = self._convert_action(index, row)
            actions[index] = json_snippet
        return actions
    
    
    def convert_row(self, event_key, event_row):
        '''Convert a single row to DSJson
        
        Parameters:
            event_key -- primary key for event row.
            event_row -- row of event data to convert, as a Pandas Series.
            
        Note: event_row must come from a DataFrame with the same schema (and index) as the DataFrame used to
        construct the DSJsonConverter.
        '''
        if (len(event_row) != len(self._event_df.columns)):
            raise ValueError(f'event_row has {len(event_row)} columns while event table has {len(self._event_df.columns)}. '
                            f'This is usually caused by not indexing the event_row data by its primary key.')
        output_json = OrderedDict()
        event_context = OrderedDict()
        
        # Collect linked action information
        actions_by_key = DSJsonPatcher._resolve_link_mappings(self._action_link_mappings, event_row)
        actions_by_index = sorted(actions_by_key.values(), key=lambda action: action['index'])
        actions_by_rank = sorted(actions_by_key.values(), key=lambda action: action['rank'])
            
        # _label_cost must always appear first (if provided), and must always be a float
        if '_label_cost' in self._event_table.toplevel_mapping:
            output_json['_label_cost'] = jsonify(event_row.iat[self._event_table.toplevel_mapping['_label_cost'][0]])
            
        # _label_probability must always be a float
        if '_label_probability' in self._event_table.toplevel_mapping:
            output_json['_label_probability'] = jsonify(event_row.iat[self._event_table.toplevel_mapping['_label_probability'][0]])
            
        # Set _label_Action and _labelindex
        label_action = self._get_label_action(event_row, actions_by_key)
        if label_action:
            output_json['_label_Action'] = int(label_action)
            output_json['_labelindex'] = int(label_action - 1)
        
        # Event primary key
        if self._event_table and self._event_table.primary_key:
            if isinstance(self._event_table.primary_key_feature, str):
                output_json[self._event_table.primary_key_feature] = event_key
            else:
                (namespace, feature) = self._event_table.primary_key_feature
                if namespace not in event_context:
                    event_context[namespace] = OrderedDict()
                event_context[namespace][feature] = event_key
        
        # Process toplevel Event mappings (other than label mappings and the primary key)
        for field,(column_index,column_name) in self._event_table.toplevel_mapping.items():
            if column_name == self._event_table.primary_key:
                continue
            if field.startswith('_label'):
                continue
            if event_row.iat[column_index]:
                output_json[field] = jsonify(event_row.iat[column_index])
                
        # Process namespace Event mappings
        for (namespace,feature),(column_index,column_name) in self._event_table.patch_mapping.items():
            if namespace not in event_context and namespace != DEFAULT_NAMESPACE:
                event_context[namespace] = OrderedDict()
            if namespace == DEFAULT_NAMESPACE:
                event_context[feature] = jsonify(event_row.iat[column_index]) 
            event_context[namespace][feature] = jsonify(event_row.iat[column_index])
        
        # Process actions.  Convert lazily if needed.
        if self._actions is None:
            self._actions = self._convert_actions()
        multi_list = []
        for action in actions_by_index:
            action_key_value = action['key']
            action_json = self._actions[action_key_value]
            multi_list.append(action_json)
        event_context['_multi'] = multi_list
        
        if 'probability' in self._action_link_mappings[2]:
            output_json['a'] = [jsonify(action['index']) for action in actions_by_rank]
        output_json['c'] = event_context
        if 'probability' in self._action_link_mappings[2]:
            output_json['p'] = [jsonify(action['probability']) for action in actions_by_rank]
        return output_json


def patch_dsjson(input_dsjson, output_dsjson, event_table=None, action_table=None, config=default_patch_config, output_every=0, stream_events=False):
    '''Main function to patch a DSJson file from an event_table and/or action_table.  See DSJsonPatcher.patch_row for a complete description
    of how patching works.
    
    Parameters:
    input_dsjson -- Input dsjson file to patch.  Treated as zipped if it ends in .gz.
    output_dsjson -- Output dsjson file containing patched data.  Will be zipped if the name ends in .gz.  May have fewer lines than the input_dsjson if
                     there are error rows or rows that get dropped because they do not match any row in the Event Table.
    event_table -- Optional Event Table.  May be a CSV file (can be zipped) or a Pandas DataFrame.  Will be used to patch the input DSJson if provided.
    action_table -- Optional Action Table.  May be a CSV file (can be zipped) or a Pandas DataFrame.  Will be used to patch the input DSJson if provided.
    config -- Configuration to control patching.  Defaults to a typical configuration useful for patching Personalizer data.
    output_every -- If set to a nonzero value, outputs status after output_every rows.  If set to 0, suppresses all output.  Defaults to 0.
    stream_events -- If set to True, processes the Event Table in streaming mode.  This assumes that the rows in the Event Table are in the same order
                     as the rows in the input dsjson file, and that they line up one-to-one (if you ignore any error rows).  This will be true if the
                     Event Table was produced by an Extract operation.
    '''
    event_df = None
    if event_table is not None and 'event_table' in config:
        index_col = config['event_table']['primary_key'] if 'primary_key' in config['event_table'] else None
        if isinstance(event_table, pd.DataFrame):
            if stream_events:
                raise ValueError('event_table may not be a DataFrame when stream_events is True')
            event_df = event_table
        elif stream_events:
            if output_every:
                print(f'Streaming events from {event_table}')
            event_iter = pd.read_csv(event_table, index_col=index_col, header=0, chunksize=1, float_precision='round_trip')
            event_df = next(event_iter)
        else:
            if output_every:
                print(f'Reading events table from {event_table}')
            event_df = pd.read_csv(event_table, index_col=index_col, float_precision='round_trip')
        
    action_df = None
    if action_table is not None and 'action_table' in config:
        if isinstance(action_table, pd.DataFrame):
            action_df = action_table
        else:
            index_col = None
            if 'primary_key' in config['action_table']:
                index_col = config['action_table']['primary_key']
            if output_every:
                print(f'Reading actions table from {action_table}')
            action_df = pd.read_csv(action_table, index_col=index_col, float_precision='round_trip')
    
    input_row_count = 0
    output_row_count = 0
    error_count = 0
    
    patcher = DSJsonPatcher(config, event_df, action_df)    
    
    with gzip.open(output_dsjson, 'wt', encoding='utf8') if output_dsjson.endswith('.gz') else open(output_dsjson, 'w', encoding='utf8') as dsjson_out:
        with gzip.open(input_dsjson, 'rt', encoding='utf8') if input_dsjson.endswith('.gz') else open(input_dsjson, 'r', encoding='utf8') as dsjson_in:
            for line in dsjson_in:
                try:
                    input_json = json.loads(line.strip())
                    if 'c' in input_json:
                        keep_row = False
                        if event_table is not None and stream_events:
                            row_df = event_df
                            event_df = next(event_iter, None)
                            keep_row = patcher.patch_row(input_json, row_df, 0)
                        else:
                            keep_row = patcher.patch_row(input_json)
                        if keep_row:
                            try:
                                dsjson_out.write(json.dumps(input_json, separators=(',', ':')))
                            except Exception as err:
                                # Hitting this indicates a bug in the code.  Wrapping the exception to provide more information.
                                raise Exception(f'Failed to convert row to JSON: {input_json}') from err
                            dsjson_out.write('\n')
                            output_row_count += 1
                    else:
                        error_count += 1
                except json.JSONDecodeError:
                    error_count += 1
                input_row_count += 1
                if output_every and input_row_count % output_every == 0:
                    print(f'{input_dsjson}: {input_row_count} rows')
    if output_every:
        print(f'Patched {input_row_count} rows of {input_dsjson} to {output_row_count} rows of {output_dsjson}')
        
    if stream_events:
        event_iter.close()

        
        
def convert_to_dsjson(output_dsjson, event_table, action_table, config=default_convert_config, output_every=0):
    '''Main function to convert an event_table and action_table into a dsjson file.  The conversion is performed by joining the two tables, using
    special columns defined in the event_table to specify keys into the action_table.  The exact specification is controlled by the supplied
    config.
    
    Parameters:
    output_dsjson -- Output dsjson file containing converted data.  Will be zipped if the name ends in .gz.
    event_table -- Event Table.  May be a CSV file (can be zipped) or a Pandas DataFrame.
    action_table -- Action Table.  May be a CSV file (can be zipped) or a Pandas DataFrame.
    config -- Configuration to control conversion.  Defaults to a typical configuration that works with tables generated by vw_tabular.
    output_every -- If set to a nonzero value, outputs status after output_every rows.  If set to 0, suppresses all output.  Defaults to 0.
    '''
    if 'event_table' not in config:
        raise ConfigError(config=config, message='Configuration must specify event_table when doing a conversion')
    if 'action_table' not in config:
        raise ConfigError(config=config, message='Configuration must specify action_table when doing a conversion')
    if 'links' not in config['event_table']:
        raise ConfigError(config=config, message='Configuration must specify links inside event_table when doing a conversion')
    
    index_col = config['event_table']['primary_key'] if 'primary_key' in config['event_table'] else None
    event_iter = None
    if isinstance(event_table, pd.DataFrame):
        event_df = event_table
        if index_col and event_table.index.name != index_col:
            event_df = event_df.set_index(index_col)
    else:
        if output_every:
            print(f'Streaming events from {event_table}')
        event_iter = pd.read_csv(event_table, index_col=index_col, header=0, chunksize=1, float_precision='round_trip')
        event_df = next(event_iter)

    if isinstance(action_table, pd.DataFrame):
        action_df = action_table
    else:
        index_col = None
        if 'primary_key' in config['action_table']:
            index_col = config['action_table']['primary_key']
        if output_every:
            print(f'Reading actions table from {action_table}')
        action_df = pd.read_csv(action_table, index_col=index_col, float_precision='round_trip')

    output_row_count = 0
    
    patcher = DSJsonPatcher(config, event_df, action_df)
    
    with gzip.open(output_dsjson, 'wt', encoding='utf8') if output_dsjson.endswith('.gz') else open(output_dsjson, 'w', encoding='utf8') as dsjson_out:
        if event_iter is None:
            for index, row in event_df.iterrows():
                output_row = patcher.convert_row(index, row)
                dsjson_out.write(json.dumps(output_row, separators=(',', ':')))
                dsjson_out.write('\n')
                output_row_count += 1
                if output_every and output_row_count % output_every == 0:
                    print(f'Generated {output_row_count} rows of {output_dsjson}')
        else:
            while event_df is not None:
                row_df = event_df
                event_df = next(event_iter, None)
                output_row = patcher.convert_row(row_df.index[0], row_df.iloc[0])
                try:
                    dsjson_out.write(json.dumps(output_row, separators=(',', ':')))
                except Exception as err:
                    raise Exception(f'Failed to convert {output_row} to json') from err
                dsjson_out.write('\n')
                output_row_count += 1
                if output_every and output_row_count % output_every == 0:
                    print(f'Generated {output_row_count} rows of {output_dsjson}')

    if output_every:
        print(f'Generated {output_row_count} rows to {output_dsjson}')
    if event_iter is not None:
        event_iter.close()
