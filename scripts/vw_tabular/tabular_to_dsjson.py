import csv
from collections import OrderedDict
import json
import pandas as pd
import numpy as np
import gzip


# This is the default patch configuration that will be used if none is explicitly specified.
# It is designed to work well with typical data coming from Personalizer.
default_patch_config = {
    "event_table": {
        # Primary key column for the event table
        "primary_key": "EventId",
        
        "columns": {
            # Toplevel column mappings.  These columns map directly to toplevel JSON elements.
            "EventId": "EventId",

            # Full wildcard.  Map all columns of the form Context.<namespace>.<feature> to the feature
            # with the given namespace.
            "Context.**": { }
        }
    },

    "action_table": {
        # Primary key column for the action table
        "primary_key": "Tag",

        "columns": {
            # Toplevel column mappings.
            "Tag": "_tag",
            
            # Full wildcard.  "namespace" defaults to "*" and can be omitted.
            "Action.**": { }
        }
    }
}


# This is the default convert configuration that will be used if none is explicitly specified.
# It is designed to work well with typical data coming from Personalizer.
default_convert_config = {
    "event_table": {
        "primary_key": "EventId",
        
        "columns": {
            # Timestamp and Version are common in DSJson records.
            "EventId": "EventId",
            "Timestamp": "Timestamp",
            "Version": "Version",
            
            # You must map the Label when doing a conversion if you want to run a counterfactual analysis using
            # the generated DSJson.  Cost and Probability are required as well.
            "Label": "_label_key",
            "Label._cost": "_label_cost",
            "Label._p": "_label_probability",
            
            # Standard context features
            "Context.**": { }
        },
        
        # Defines how actions are linked to events.
        "links": {
            # Scheme must be rank (for now).  Rank is also the default.
            "scheme": "rank",
            
            # By default, linked actions use columns as follows:
            # ActionRank.<rank> contains the key of the linked action.  Rank is the order in which this action was chosen by VW.
            # ActionRank.<rank>._index contains the index of the linked action.  Index is the order in which this action should appear in a DSJson file.
            # ActionRank.<rank>._p contains the probability of the linkex action.  This is the probability assigned to this action by VW.
            "columns": {
                "ActionRank.*": "key",
                "ActionRank.*._index": "index",
                "ActionRank.*._p": "probability"
            }
        }
    },
    
    "action_table": {
        # Primary key column for the action table
        "primary_key": "Tag",

        "columns": {
            # Tag appears as a toplevel feature
            "Tag": "_tag",
            
            # Standard action features
            "Action.**": { }
        }
    }
}


def jsonify(val):
    '''Convert a scalar to a type that is JSON-serializable.  Specifically designed for numpy-types lik int46, which
    generally aren't JSON-serializable on their own.
    
    Parameters:
    val -- value to convert.  May be a Numpy type.
    
    Returns:
    output value.  Numpy numbers will be converted to standard ints and floats.
    '''
    if isinstance(val, np.integer):
        return int(val)
    elif isinstance(val, np.floating):
        return float(val)
    else:
        return val


class ConfigError(Exception):
    '''Exception class for errors encountered while processing configuration.
    
    Parameters (also available as public members)
    config -- configuration object containing the error.
    message -- additional messaging describing the error.
    '''
    def __init__(self, config, message):
        self.config = config
        self.message = message
        
    def __str__(self):
        return f'{self.message}.  Config = {json.dumps(self.config)}'


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
        self._event_toplevel_mapping = {}
        self._event_column_mapping = {}
        self._event_json_key = None
        self._event_primary_key = None
        
        self._action_df = action_df
        self._action_toplevel_mapping = {}
        self._action_column_mapping = {}
        self._action_json_key = None
        self._action_primary_key = None
        
        self._action_link_mappings = None
        
        event_config = config['event_table'] if 'event_table' in config else None
        event_columns = event_config['columns'] if event_config and 'columns' in event_config else {}
        action_config = config['action_table'] if 'action_table' in config else None
        action_columns = action_config['columns'] if action_config and 'columns' in action_config else {}

        # Event table config
        if self._event_df is not None:
            (self._event_toplevel_mapping, self._event_column_mapping) = \
            self._get_column_mappings(event_columns, self._event_df.columns, self._event_df.index.name)

            # Check the event primary key, and add an identity mapping for it if one isn't set already.
            try:
                self._event_primary_key = event_config['primary_key']
                if self._event_primary_key == event_df.index.name:
                    pass
                elif self._event_primary_key not in self._event_df.columns:
                    raise ConfigError(config=config, 
                                      message=f'primary key "{self._event_primary_key}" is not defined as either '
                                      f'the index ({self._event_df.index.name}) '
                                      f'or as a column in the Event Table ({self._event_df.columns})')
                else:
                    self._event_df = self._event_df.set_index(self._event_primary_key)
                
                # Get the column mapping for the primary key.  If there isn't one, add an identity mapping for the primary key
                if self._event_primary_key in event_columns:
                    self._event_json_key = event_columns[self._event_primary_key]
                else:
                    self._event_toplevel_mapping[self._event_primary_key] = self._event_primary_key
                    self._event_json_key = self._event_primary_key  
            except KeyError as err:
                raise ConfigError(config=event_config, message=f'primary_key must be defined in event_table') from err
        
        # Action table config
        if self._action_df is not None:
            (self._action_toplevel_mapping, self._action_column_mapping) = \
            self._get_column_mappings(action_columns, self._action_df.columns, self._action_df.index.name)
            
            # Check the action primary key, and add an identity mapping for it if one isn't set already.
            try:
                self._action_primary_key = action_config['primary_key']
                if self._action_primary_key == action_df.index.name:
                    pass
                elif self._action_primary_key not in self._action_df.columns:
                    raise ConfigError(config=config, 
                                      message=f'primary key "{self._action_primary_key}" is not defined as either '
                                      f'the index ({self._action_df.index.name}) '
                                      f'or as a column in the Action Table ({self._action_df.columns})')
                else:
                    self._action_df = self._action_df.set_index(self._action_primary_key)
                
                # Get the column mapping for the pimary key.  If there isn't one, add an identity mapping for the primary key
                if self._action_primary_key in action_columns:
                    self._action_json_key = action_columns[self._action_primary_key]
                else:
                    self._action_toplevel_mapping[self._action_primary_key] = self._action_primary_key
                    self._action_json_key = self._action_primary_key
            except KeyError as err:
                raise ConfigError(config=action_config, message=f'primary_key must be defined in action_table') from err
                
        # Event to action links
        if event_config and 'links' in event_config and self._event_df is not None:
            self._action_link_mappings = DSJsonPatcher._get_link_mappings(event_config['links'], self._event_df.columns)


    @staticmethod
    def _get_column_mappings(column_config, data_columns, index_name=None):
        '''Helper function to construct column mappings from configuration and from the list of columns in a dataframe.
        May be used on either the Event Table or the Action Table.
        
        Parameters:
        column_config -- the section of the configuration object defining the column mappings to process.
        data_columns -- list of column names from the data to be mapped.
        index_name -- (optional) name of the index column, if any.
        
        Returns:
        A tuple of (toplevel_mapping, column_mapping), where:
        toplevel_mapping -- mapping of toplevel columns, as a dict mapping the JSON field name of the toplevel column to the string
                            identifying the column name in the provided dataframe.
        column_mapping -- mapping of feature columns, as a dict mapping a tuple of (namespace, feature) to the corresponding column
                          name in the provided dataframe.
        '''
        toplevel_mapping = {}
        column_mapping = {}
        
        for key, value in column_config.items():
            # Toplevel column mappings.  Toplevel mappings always map to strings (not dicts).
            if isinstance(value, str):
                if '*' in value:
                    raise ConfigError(config=column_config, message=f'Wildcard "{value}" may not map to a toplevel field')
                if value in toplevel_mapping:
                    raise ConfigError(config=column_config, message=f'Duplicate toplevel mapping to "{value}" in configuration')
                if key not in data_columns and key != index_name:
                    raise ConfigError(config=column_config, message=f'Toplevel mapping "{key}" is not defined as a column or index')
                toplevel_mapping[value] = key
                    
            # Full wildcard mappings.
            elif key.endswith('**'):
                if len(key) < 4:
                    raise ConfigError(config=column_config, message=f'Full wildcard "{key}" must have at least two characters before "**"')
                if not isinstance(value, dict):
                    raise ConfigError(config=column_config, message=f'Full wildcard "{key}" must be set to a dict')
                family_name = key[0:-3]
                separator = key[-3]
                for column in data_columns:
                    # If the column isn't of the form <family>.<namespace>.<feature>, then it doesn't match the full wildcard
                    if not column.startswith(family_name + separator):
                        continue
                    namespace_and_feature = column[len(key)-2:]
                    separator_pos = namespace_and_feature.find(separator)
                    if separator_pos >= 0:
                        raw_namespace = namespace_and_feature[:separator_pos]
                        if 'namespace' in value:
                            if not '*' in value['namespace']:
                                raise ConfigError(config=column_config,
                                                  message=f'Full wildcard "{key}" value "{value}" must specify a wildcard for "namespace"')
                            namespace = value['namespace'].replace('*', raw_namespace)
                        else:
                            namespace = raw_namespace                      
                        feature = namespace_and_feature[separator_pos+1:]
                        column_mapping[(namespace,feature)] = column

            # Single wildcard mappings.
            elif key.endswith('*'):
                if len(key) < 3:
                    raise ConfigError(config=column_config, message=f'Wildcard "{key}" must have at least two characters before "*"')
                if not isinstance(value, dict):
                    raise ConfigError(config=column_config, message=f'Wildcard "{key}" must be set to a dict')
                if 'namespace' not in value or '*' in value['namespace']:
                    raise ConfigError(config=column_config, message=f'Wildcard "{key}" value "{value}" namespace must be specified '
                                      f'and not contain a wildcard')
                family_name = key[0:-2]
                separator = key[-2]
                for column in data_columns:
                    # If the column isn't of the form <family>.<feature>, then it doesn't match the wildcard
                    if not column.startswith(family_name + separator):
                        continue
                    raw_feature = column[len(key)-1:]
                    if 'feature' in value:
                        if not '*' in value['feature']:
                            raise ConfigError(config=column_config,
                                              message=f'Wildcard "{key}" value "{value}" must specify a wildcard for "feature"')
                            feature = value['feature'].replace('*', raw_feature)
                    else:
                        feature = raw_feature                     
                    namespace = value['namespace']
                    column_mapping[(namespace,feature)] = column
                            
            # Single column mapping.
            else:
                if key not in data_columns:
                    raise ConfigError(config=column_config, message=f'Feature mapping "{key}" is not defined as a column')
                if not isinstance(value, dict):
                    raise ConfigError(config=column_config, message=f'Feature mapping "{key}" must be set to a dict')
                if 'feature' not in value or '*' in value['feature']:
                    raise ConfigError(config=column_config,
                                      message=f'Feature mapping "{key}" value "{value}" must specify "feature" and not contain a wildcard')
                feature = value['feature']
                namespace = '_'
                if 'namespace' in value:
                    namespace = value['namespace']
                    if '*' in namespace:
                        raise ConfigError(config=column_config, message=f'Feature mapping "{key}" value "{value}" '
                                          f'namespace may not contain a wildcard')
                column_mapping[(namespace,feature)] = key
        
        return toplevel_mapping, column_mapping

    
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
        link_mapping -- a dict mapping the link identifier (e.g. rank) to the mapping (e.g. 'key', 'index', 'probability') to the
                        column name.
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
            for column in columns:
                if column.startswith(first_part) and column.endswith(last_part):
                    end_pos = column.find(last_part)
                    if end_pos > 0:
                        wildcard_value = column[wildcard_pos:end_pos]
                    else:
                        wildcard_value = column[wildcard_pos:]
                    try:
                        wildcard_value = int(wildcard_value)
                    except ValueError as err:
                        continue
                    if wildcard_value not in link_mapping:
                        link_mapping[wildcard_value] = {}
                    link_mapping[wildcard_value][value] = column

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
        (scheme, link_mapping, mapped_fields) = link_mapping_triple
        actions_by_key = {}
        for link_identifier, link_fields in link_mapping.items():
            linked_action = {}
            linked_action[scheme] = link_identifier
            for link_field, column in link_fields.items():
                linked_action[link_field] = data_row.at[column]
            if linked_action['index'] is None:
                linked_action['index'] = linked_action['rank']
            actions_by_key[linked_action['key']] = linked_action
        
        return actions_by_key


    def _patch_dict(self, namespace, json_dict, data_row, column_mapping, mapped_columns):
        '''Helper function to patch values within a JSON dict.  Includes special logic to handle the
        special _multi field that the DSJSon format uses to manage actions.
        
        Parameters:
        namespace -- Name of the current namespace, or '_' if this is the default namespace.
        json_dict -- Dict to patch.
        data_row -- Row of Pandas data from which to patch.
        column_mapping -- Mapping from tuples of (namespace, feature) to column names in the data_row.
        mapped_columns -- Set tracking columns which have been used to patch values.
        
        Returns:
        A patched copy of json_dict.
        '''
        output_dict = OrderedDict()
        for key,value in json_dict.items():
            # Handle the '_multi' key specially if it shows up at the toplevel
            if key == '_multi' and namespace == '_':
                output_dict[key] = self._patch_multi(value)
            
            # Pass other "special" keys through unchanged
            elif key.startswith('_'):
                output_dict[key] = value
                
            else:
                # Recurse if a dict
                if isinstance(value, dict):
                    output_dict[key] = self._patch_dict(key, value, data_row, column_mapping, mapped_columns)
                    
                # Or a list
                elif isinstance(value, list):
                    output_dict[key] = self._patch_list(key, value, data_row, column_mapping, mapped_columns)
                    
                # Matched a column mapping?
                elif (namespace,key) in column_mapping:
                    column = column_mapping[(namespace,key)]
                    mapped_columns.add(column)
                    try:
                        output_value = data_row.at[column]
                    except Exception as err:
                        raise Exception(f'Failed to retrieve column {column} from row {data_row}') from err
                    if output_value is not None:
                        output_dict[key] = jsonify(output_value)
                    
                # Don't patch
                else:
                    output_dict[key] = value
        return output_dict
                    
                    
    def _patch_list(self, namespace, json_list, data_row, column_mapping, mapped_columns):
        '''Helper function to patch values within a JSON list.

        Parameters:
        namespace -- Name of the current namespace, or '_' if this is the default namespace.
        json_list -- List to patch.
        data_row -- Row of Pandas data from which to patch.
        column_mapping -- Mapping from tuples of (namespace, feature) to column names in the data_row.
        mapped_columns -- Set tracking columns which have been used to patch values.
        
        Returns:
        A patched copy of json_list.
        '''
        output_list = []
        for value in json_list:
            if isinstance(value, dict):
                output_list.append(self._patch_dict(namespace, value, data_row, column_mapping, mapped_columns))
            elif isinstance(value, list):
                output_list.append(self._patch_list(namespace, value, data_row, column_mapping, mapped_columns))
            else:
                output_list.append(value)
        return output_list
    
    
    def _patch_multi(self, multi_list):
        '''Helper function to patch the special _multi list in a DSJson document.  The _multi list is embedded inside
        the toplevel context namespace dict ('c'), although it actually contains action features rather than event
        features.
        
        Parameters:
        multi_list -- _multi list from the DSJson document.
        
        Returns:
        A patched copy of multi_list.
        '''
        result = []
        for action_dict in multi_list:
            # Find the row of Action table data corresponding to this action.  If we can't then don't patch.
            action_row = None
            if self._action_df is not None:
                if self._action_json_key not in action_dict:
                    result.append(action_dict)
                    continue
                action_primary_key_value = action_dict[self._action_json_key]
                action_row = self._action_df.loc[action_primary_key_value]
                
            # Map columns in the Action table
            mapped_action_columns = set()
            output_dict = OrderedDict()
            for key,value in action_dict.items():
                if key in self._action_toplevel_mapping:
                    column_name = self._action_toplevel_mapping[key]
                    mapped_action_columns.add(column_name)
                    if column_name == self._action_primary_key:
                        output_dict[key] = action_primary_key_value
                    else:
                        output_dict[key] = jsonify(action_row.at[column_name])
                        
                elif isinstance(value, list):
                    output_dict[key] = self._patch_list(key, value, action_row, self._action_column_mapping, mapped_action_columns)
                    
                elif isinstance(value, dict):
                    output_dict[key] = self._patch_dict(key, value, action_row, self._action_column_mapping, mapped_action_columns)
                    
                elif ('_',key) in self._action_column_mapping:
                    column = action_column_mapping[('_',key)]
                    mapped_action_columns.add(column)
                    output_value = action_row[column]
                    if output_value is not None:
                        output_dict[key] = jsonify(output_value)
                    
                else:
                    output_dict[key] = value
                    
            # Add new action features by applying any unused event mappings
            if action_row is not None:
                for field,column in self._action_toplevel_mapping.items():
                    if column not in mapped_action_columns and action_row.at[column]:
                        output_dict[field] = jsonify(action_row.at[column])
                for (namespace,feature),column in self._action_column_mapping.items():
                    if column not in mapped_action_columns and action_row.at[column]:
                        if namespace not in output_dict and namespace != '_':
                            output_dict[namespace] = OrderedDict()
                        if namespace == '_':
                            output_dict[feature] = jsonify(action_row.at[column])
                        else:
                            # Special case: if the namespace already exists but points to a list, iterate through until we we a dict
                            namespace_dict = output_dict[namespace]
                            while isinstance(namespace_dict, list):
                                if len(namespace_dict) == 0:
                                    namespace_dict.append(OrderedDict())
                                namespace_dict = namespace_dict[-1]
                            namespace_dict[feature] = jsonify(action_row.at[column])
                
            result.append(output_dict)
            
        return result
    
    
    def _get_label_action(self, event_row, actions_by_key):
        '''This helper function computes the index of the label (aka chosen action) from one of three possible columns
        in the event_row (depending on how column mappings are configured).  The possible columns are (in priority order):
        1. _label_key.  Identifies the label by its key.
        2. _label_Action.  Identifies the label by a 1-based index.
        3. _label_index.  Identifies the label by a 0-based index.
        
        Parameters:
        event_row -- Pandas row containing the event.
        actions_by_key -- Action links, mapped by key.
        
        Returns:
        The 1-based index on the label, or None if no label was specified or the label was specified by a key that isn't an action
        in this row.
        '''
        if '_label_key' in self._event_toplevel_mapping:
            label_key = event_row.at[self._event_toplevel_mapping['_label_key']]
            if label_key in actions_by_key:
                return actions_by_key[label_key]['index']
            
        if '_label_Action' in self._event_toplevel_mapping:
            return event_row.at[self._event_toplevel_mapping['_label_Action']]
        
        if '_labelindex' in self._event_toplevel_mapping:
            return event_row.at[self._event_toplevel_mapping['_labelindex'] + 1]
        
        return None
                
                
    def patch_row(self, input_json, event_row=None):
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
        input_json -- input row of data, in DSJson format.
        event_row -- optional Pandas row containing event data for patching this row.
        
        Returns:
        A patched DSJson row of data.
        '''
        output_json = OrderedDict()
        
        # If an explicit Event Row isn't specified, try to look it up in the Event Table using this row's primary key.
        # If the Event Table was specified and we still can't find an Event Row, just drop this line of input.
        # If the Event primary key matches multiple rows, use the first match.
        if event_row is not None:
            event_primary_key_value = input_json[self._event_json_key]
        elif self._event_df is not None:
            if self._event_json_key not in input_json:
                return None
            event_primary_key_value = input_json[self._event_json_key]
            try:
                event_row = self._event_df.loc[event_primary_key_value]
                if event_row.ndim > 1:
                    event_row = event_row.iloc[0]
            except KeyError:
                return None
        
        mapped_event_columns = set()
        
        # Map columns in the Event Table
        for key,value in input_json.items():
            # Toplevel columns.  Primary key is set as the index on the dataframe so treat as a special case
            if key in self._event_toplevel_mapping:
                column_name = self._event_toplevel_mapping[key]
                mapped_event_columns.add(column_name)
                if column_name == self._event_primary_key:
                    output_json[key] = event_primary_key_value
                else:
                    output_json[key] = jsonify(event_row.at[column_name])
                        
            elif key == 'c':
                output_json[key] = self._patch_dict('_', value, event_row, self._event_column_mapping, mapped_event_columns)
                
            else:
                output_json[key] = value

        # Add new event features by applying any unused event mappings
        if event_row is not None:
            for field,column in self._event_toplevel_mapping.items():
                if column not in mapped_event_columns and event_row.at[column]:
                    output_json[field] = jsonify(event_row.at[column])
            for (namespace,feature),column in self._event_column_mapping.items():
                if column not in mapped_event_columns and event_row.at[column]:
                    if namespace not in output_json['c'] and namespace != '_':
                        output_json['c'][namespace] = OrderedDict()
                    if namespace == '_':
                        output_json['c'][feature] = jsonify(event_row.at[column])
                    else:
                        # Special case: if the namespace already exists but points to a list, iterate through until we we a dict
                        namespace_dict = output_json['c'][namespace]
                        while isinstance(namespace_dict, list):
                            if len(namespace_dict) == 0:
                                namespace_dict.append(OrderedDict())
                            namespace_dict = namespace_dict[-1]
                        namespace_dict[feature] = jsonify(event_row.at[column])
        
        return output_json
    
    
    def _convert_action(self, action_key_column, action_key, action_row):
        '''Convert a row of action data to DSJson
        
        Parameters:
            action_row: Row of data, in Pandas format.
        
        Returns:
            the action row, in DSJson.
        '''
        output_json = OrderedDict()
        
        # Process toplevel Action mappings
        for field,column in self._action_toplevel_mapping.items():
            if column == action_key_column:
                output_json[field] = jsonify(action_key)
            else:
                output_json[field] = jsonify(action_row.at[column])
                
        # Process namespace Event mappings
        for (namespace,feature),column in self._action_column_mapping.items():
            if namespace not in output_json and namespace != '_':
                output_json[namespace] = OrderedDict()
            if namespace == '_':
                output_json[feature] = jsonify(action_row.at[column]) 
            output_json[namespace][feature] = jsonify(action_row.at[column])
            
        return output_json

    
    def convert_actions(self, action_table):
        '''Convert an action table (represented as a Pandas DataFrame) to dsjson.
        
        Parameters:
            action_table -- Pandas DataFrame containing all action data.
        
        Returns:
            a mapping from action primary key to DSJson snippet for each action.
        '''
        actions = OrderedDict()
        for index, row in action_table.iterrows():
            json_snippet = self._convert_action(action_table.index.name, index, row)
            actions[json_snippet[self._action_json_key]] = json_snippet
        return actions
    
    
    def convert_row(self, actions, event_row):
        '''Convert a single row to DJSon
        
        Parameters:
            actions -- converted actions, as a dict mapping action keys to action JSon lines.
            event_row -- row of event data to convert, as a Pandas row.
        '''
        output_json = OrderedDict()
        
        # Collect linked action information
        actions_by_key = DSJsonPatcher._resolve_link_mappings(self._action_link_mappings, event_row)
        actions_by_index = sorted(actions_by_key.values(), key=lambda action: action['index'])
        actions_by_rank = sorted(actions_by_key.values(), key=lambda action: action['rank'])
            
        # _label_cost must always appear first (if provided), and must always be a float
        if '_label_cost' in self._event_toplevel_mapping:
            output_json['_label_cost'] = float(event_row.at[self._event_toplevel_mapping['_label_cost']])
            
        # _label_probability must always be a float
        if '_label_probability' in self._event_toplevel_mapping:
            output_json['_label_probability'] = float(event_row.at[self._event_toplevel_mapping['_label_probability']])
            
        # Set _label_Action and _labelindex
        label_action = self._get_label_action(event_row, actions_by_key)
        if label_action:
            output_json['_label_Action'] = int(label_action)
            output_json['_labelindex'] = int(label_action - 1)
        
        # Process toplevel Event mappings (other than label mappings)
        for field,column in self._event_toplevel_mapping.items():
            if not field.startswith('_label') and event_row.at[column]:
                output_json[field] = jsonify(event_row.at[column])
                
        # Process namespace Event mappings
        event_context = OrderedDict()
        for (namespace,feature),column in self._event_column_mapping.items():
            if namespace not in event_context and namespace != '_':
                event_context[namespace] = OrderedDict()
            if namespace == '_':
                event_context[feature] = jsonify(event_row.at[column]) 
            event_context[namespace][feature] = jsonify(event_row.at[column])
        
        # Process actions
        multi_list = []
        for action in actions_by_index:
            action_key_value = action['key']
            action_json = actions[action_key_value]
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
        if isinstance(event_table, pd.DataFrame):
            if stream_events:
                raise ValueError('event_table may not be a DataFrame when stream_events is True')
            event_df = event_table
        elif stream_events:
            if output_every:
                print(f'Streaming events from {event_table}')
            event_iter = pd.read_csv(event_table, header=0, chunksize=1)
            event_df = next(event_iter)
        else:
            index_col = None
            if 'primary_key' in config['event_table']:
                index_col = config['event_table']['primary_key']
            if output_every:
                print(f'Reading events table from {event_table}')
            event_df = pd.read_csv(event_table, index_col=index_col)
        
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
            action_df = pd.read_csv(action_table, index_col=index_col)
    
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
                        if event_table is not None and stream_events:
                            row_df = event_df
                            event_df = next(event_iter, None)
                            output_row = patcher.patch_row(input_json, row_df.iloc[0])
                        else:
                            output_row = patcher.patch_row(input_json)
                        if output_row:
                            try:
                                dsjson_out.write(json.dumps(output_row, separators=(',', ':')))
                            except Exception as err:
                                raise Exception(f'Failed to convert row to JSON: {output_row}') from err
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
        
    if isinstance(event_table, pd.DataFrame):
        event_df = event_table
    else:
        if output_every:
            print(f'Streaming events from {event_table}')
        event_iter = pd.read_csv(event_table, header=0, chunksize=1)
        event_df = next(event_iter)

    if isinstance(action_table, pd.DataFrame):
        action_df = action_table
    else:
        index_col = None
        if 'primary_key' in config['action_table']:
            index_col = config['action_table']['primary_key']
        if output_every:
            print(f'Reading actions table from {action_table}')
        action_df = pd.read_csv(action_table, index_col=index_col)

    output_row_count = 0
    
    patcher = DSJsonPatcher(config, event_df, action_df)
    actions = patcher.convert_actions(action_df)
    
    with gzip.open(output_dsjson, 'wt', encoding='utf8') if output_dsjson.endswith('.gz') else open(output_dsjson, 'w', encoding='utf8') as dsjson_out:
        while event_df is not None:
            row_df = event_df
            event_df = next(event_iter, None)
            output_row = patcher.convert_row(actions, row_df.iloc[0])
            dsjson_out.write(json.dumps(output_row, separators=(',', ':')))
            dsjson_out.write('\n')
            output_row_count += 1

    if output_every:
        print(f'Generated {output_row_count} rows of {output_dsjson}')

    event_iter.close()
