# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

'''Module of common code used by the converters from dsjson format to tabular and back again
'''

import json
from collections import OrderedDict
import numpy as np

# Placeholder string for representing the default namespace when a namespace name is needed
DEFAULT_NAMESPACE = '_'


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


# This configuration is similar to default_patch_config except that it will not add any additional
# features that were not in the original dsjson.  Patching using this configuration will perform
# much faster if you know you do not want to add any new features.
# It is designed to work well with typical data coming from Personalizer.
patch_only_config = {
    "event_table": {
        # Primary key column for the event table
        "primary_key": "EventId",
        
        "patch": {
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

        "patch": {
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
            # ActionRank.<rank>._p contains the probability of the linked action.  This is the probability assigned to this action by VW.
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


# Use this to create a default configuration on-the-fly from a few parameters.
def create_default_config(full_transform, changing_actions, action_key_feature):
    '''Create a default configuration based on the value of several legacy parameters.
    
    Parameters:
    full_transform -- True to perform a full transformation into Event and Action tables that can later be rejoined.  False simply to extract.
    changing_actions -- True to model slowly-changing actions.
    action_key_feature -- Identifies the feature to use as an action key, either a simple string for a toplevel field or a pair of (namespace, feature).
    '''
    config = {
        "event_table": {
            "primary_key": "EventId",
        
            "columns": {
                "EventId": "EventId",
                "Timestamp": "Timestamp",
                "Version": "Version",

                'Label': '_label_key',
                'Label._cost': "_label_cost",
                'Label._p': "_label_probability",

                'Context.**': { }                    
            }
        },
                
        "action_table": {
            "primary_key": "Tag",

            "columns": {
                "Tag": "_tag",
                'Action.**': { }
            }
        }
    }
            
    if full_transform:
        config["event_table"]["links"] = {
            "scheme": "rank",
            "columns": {
                'ActionRank.*': "key",
                'ActionRank.*._index': "index",
                'ActionRank.*._p': "probability"
            }
        }
        if changing_actions:
            config["event_table"]["links"]["columns"]['ActionRank.*._key'] = "surrogate"
                    
    if changing_actions:
        # Create mappings for the three "magic" changing actions columns: _surrogate, _timestamp, and _latest.
        config["action_table"]["columns"]["Key"] = "_surrogate"
        config["action_table"]["columns"]["Timestamp"] = "_timestamp"
        config["action_table"]["columns"]["Latest"] = "_latest"
        
    if action_key_feature:
        if isinstance(action_key_feature, str):
            config["action_table"]["columns"]["Tag"] = action_key_feature
        else:
            (namespace, feature) = action_key_feature
            config["action_table"]["columns"]["Tag"] = { "namespace": namespace, "feature": feature }     
        
    return config
    
    
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
    
    
def merge_dicts(d1, d2):
    '''Helper to merge two dictionaries.  If the same key appears in both d1 and d2, prefer the value from d1.
    
    Parameters:
    d1 -- first dict to merge.
    d2 -- second dict to merge.
    
    Returns:
    A combination of the keys and values from both dicts.
    '''
    return {**d1, **d2}


def _namespacify_dict(result, ns, obj):
    for key, value in obj.items():
        if key.startswith('_'):
            continue
        if isinstance(value, dict):
            _namespacify_dict(result, key, value)      
        elif isinstance(value, list):
            _namespacify_list(result, key, value)        
        else:
            result[(ns, key)] = value

            
def _namespacify_list(result, ns, l):
    for value in l:
        if isinstance(value, dict):
            _namespacify_dict(result, ns, value)
            
        elif isinstance(value, list):
            _namespacify_list(result, ns, value)
            
        else:
            # Ignore stray values in lists
            pass

        
def namespacify(json_snippet):
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
    if isinstance(json_snippet, list):
        _namespacify_list(result, DEFAULT_NAMESPACE, json_snippet)
    elif isinstance(json_snippet, dict):
        _namespacify_dict(result, DEFAULT_NAMESPACE, json_snippet)
    return result


def get_match(s, wildcard):
    '''Match string s against a wildcard expression which must contain '*'.  If s matches the expression, return the
    portion of s corresponding to '*'.  Else return None.
    
    Parameters:
    s -- string to match.
    wildcard -- wildcard expression.  Must contain '*'.
    
    Returns:
    The portion of s that corresponds to '*' in wildcard, or None if no match.
    '''
    wpos = wildcard.find('*')
    if wpos < 0:
        raise ValueError(f'Wildcard string \'{wildcard}\' must contain \'*\'')
    left = wildcard[:wpos]
    right = wildcard[wpos+1:]
    if s.startswith(left) and s.endswith(right):
        return s[len(left):-len(right)] if len(right) > 0 else s[len(left):]
    else:
        return None


def process_config_columns(column_config, primary_key_column):
    '''Process a column mapping config into a simplified format.  Also validates the
    config, raising ConfigError as appropriate.
    
    Parameters:
    column_config -- section of the Extract Config containing column mappings for a table.
    primary_key_column -- name of primary key column.
    
    Returns:
    A tuple of (toplevel_dict, column_dict, primary_key_feature).
    toplevel_dict is a string-to-string dict mapping toplevel_feature_name --> column_name.
    column_dict is a dict in a format ready to use with _extract_features.  See below for the full structure of this dict.
    primary_key_feature is either a string identifying the toplevel attribute mapped to primary key, or a pair of (namespace,feature)
         identifying the feature mapped to primary key, or None if there is no primary key mapping.
     
    column_dict (return value) structure:
    column_dict is a mapping identifying how to convert features identified by pairs of (namespace,feature) into columns.
        The mapping may contain three types of entries:
        * Single feature mappings map (namespace,feature) --> [column_name].  The single feature will be extracted.
        * Namespace mappings map namespace --> [(feature_wildcard, column_wildcard)].
            Every feature in the namespace that matches feature_wildcard will be extracted.  The column name is computed as follows:
                Match the feature against feature_wildcard, and determine the value of '*'.
                Replace '*' in the column_wildcard based on this value.
        * Full mappings map () --> [(namespace_wildcard, feature_wildcard, column_wildcard)].
            Every matching feature in every matching will be extracted.  Features and namespaces match if they match their respective
            wildcards.  To construct the column name, first we extract the portions of the namespace and feature that correspond to '*'
            in their wildcards.  Then we replace '**' in column_wildcard with <n>.<f> where <n> is the value extracted from the
            namespace and <f> is the value extracted from the feature, and '.' is the separator character that occurs right before the
            '**' in column_wildcard.
            Full mappings will not match features in the default namespace.  Use a namespace or single feature mapping for these.
        The right-hand side of each mapping is a list.  All mappings in the list will be applied.
        Multiple mappings may match the same feature, in which case the feature will be mapped into multiple columns.
    '''
    toplevel_dict = OrderedDict()
    column_dict = OrderedDict()
    primary_key_feature = None
    
    for column_spec, feature_spec in column_config.items():
        if not isinstance(column_spec, str):
            raise ConfigError(config=column_config, message=f'Mapped column specification {column_spec} must be a string')
            
        # Toplevel column mappings.  Toplevel mappings always map to strings (not dicts).  Skip the primary key.
        if isinstance(feature_spec, str):
            if '*' in column_spec:
                raise ConfigError(config=column_config, message=f'Wildcard "{column_spec}" may not map to a toplevel field')
            if feature_spec in toplevel_dict:
                raise ConfigError(config=column_config, message=f'Duplicate toplevel mapping to "{feature_spec}" in configuration')
            if column_spec == primary_key_column:
                primary_key_feature = feature_spec
            else:
                toplevel_dict[feature_spec] = column_spec
                    
        # Full wildcard mappings.
        elif column_spec.endswith('**'):
            if len(column_spec) < 4:
                raise ConfigError(config=column_config, message=f'Full wildcard "{column_spec}" must have at least two characters before "**"')
            if not isinstance(feature_spec, dict):
                raise ConfigError(config=column_config, message=f'Full wildcard "{feature_spec}" must be set to a dict')
            namespace_wildcard = feature_spec['namespace'] if 'namespace' in feature_spec else '*'
            if '*' not in namespace_wildcard:
                raise ConfigError(config=column_config, message=f'Full wildcard "{column_spec}": namespace in {feature_spec} must be a wildcard')
            feature_wildcard = feature_spec['feature'] if 'feature' in feature_spec else '*'
            if '*' not in feature_wildcard:
                raise ConfigError(config=column_config, message=f'Full wildcard "{column_spec}": feature in {feature_spec} must be a wildcard')
            if () not in column_dict:
                column_dict[()] = []
            column_dict[()].append((namespace_wildcard, feature_wildcard, column_spec))

        # Single wildcard mappings.
        elif column_spec.endswith('*'):
            if len(column_spec) < 3:
                raise ConfigError(config=column_config, message=f'Wildcard "{column_spec}" must have at least two characters before "*"')
            if not isinstance(feature_spec, dict):
                raise ConfigError(config=column_config, message=f'Wildcard "{column_spec}" must be set to a dict')
            if 'namespace' not in feature_spec:
                namespace = DEFAULT_NAMESPACE
            elif '*' in feature_spec['namespace']:
                raise ConfigError(config=column_config, message=f'Wildcard "{column_spec}": namespace in "{feature_spec}" must '
                                      f'not contain a wildcard')
            else:
                namespace = feature_spec['namespace']
            feature_wildcard = feature_spec['feature'] if 'feature' in feature_spec else '*'
            if '*' not in feature_wildcard:
                raise ConfigError(config=column_config, message=f'Wildcard "{column_spec}": feature in {feature_spec} must be a wildcard')
            if namespace not in column_dict:
                column_dict[namespace] = []
            column_dict[namespace].append((feature_wildcard, column_spec))
                            
        # Single column mapping.
        else:
            if not isinstance(feature_spec, dict):
                raise ConfigError(config=column_config, message=f'Feature mapping "{column_spec}" must be set to a dict')
            namespace = feature_spec['namespace'] if 'namespace' in feature_spec else DEFAULT_NAMESPACE
            if '*' in namespace:
                raise ConfigError(config=column_config, message=f'Feature mapping "{column_spec}": namespace in "{feature_spec}" must not contain a wildcard')
            if 'feature' not in feature_spec or '*' in feature_spec['feature']:
                raise ConfigError(config=column_config, message=f'Feature mapping "{column_spec}": feature in "{feature_spec}" must be specified '
                                      f'and not contain a wildcard')
            feature = feature_spec['feature']
            if (namespace, feature) not in column_dict:
                column_dict[(namespace, feature)] = []
            if column_spec == primary_key_column:
                primary_key_feature = (namespace, feature)
            else:
                column_dict[(namespace, feature)].append(column_spec)
    
    return (toplevel_dict, column_dict, primary_key_feature)


def process_event_links(links_config):
    '''Process the links section of an event_table configuration.  This is a very specific config section used to determine
    how the links to the various actions referenced by an event will appear in the event table.
    The result is a string-->string map.  The right-hand-side of the map is a wildcard expression.  The left-hand-side is
    one of five "special" features: rank, key, index, probability, and surrogate.
    '''
    event_links = OrderedDict()
    
    # Process the scheme.  For now must be 'rank'.  The scheme feature gets mapped to the special wildcard '*'.
    if 'scheme' in links_config and links_config['scheme'] != 'rank':
        raise ConfigError(config=links_config, message='Event table links configuration scheme must be "rank"')
    event_links['rank'] = '*'
    
    # Process other mappings.  Duplicates are not allowed.
    if 'columns' not in links_config:
        raise ConfigError(config=links_config, message='Links configuration must specify columns')
    if not isinstance(links_config['columns'], dict):
        raise ConfigError(config=links_config, message='Links configuration columns must be a dict')
    allowed_features = ['key', 'index', 'probability', 'surrogate']
    for column_wildcard, feature in links_config['columns'].items():
        if feature not in allowed_features:
            raise ConfigError(config=links_config, message=f'Illegal links configuration mapping to "{feature}"; must be one of {allowed_features}')
        if feature in event_links:
            raise ConfigError(config=links_config, message=f'Duplicate links configuration mapping to "{feature}"')
        if '*' not in column_wildcard or column_wildcard == '*':
            raise ConfigError(config=links_config, message=f'Illegal links configuration wildcard "{column_wildcard}"')
        event_links[feature] = column_wildcard
    return event_links
    

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
