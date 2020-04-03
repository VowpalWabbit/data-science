import json
from collections import OrderedDict
import uuid
from datetime import datetime, timezone


MULTI_KEY = '_multi'
TAG_KEY = '_tag'
LABEL_KEY = '_label'
TEXT_KEY = '_text'
LABEL_COST_KEY = '_label_cost'
LABEL_PROBABILITY_KEY = '_label_probability'
LABEL_ACTION_KEY = '_label_Action'
LABEL_INDEX_KEY = '_labelIndex'
EVENT_ID_KEY = 'EventId'
TIMESTAMP_KEY = 'Timestamp'


GENERIC_SHARED_FEATURES_KEY = 'shared_features'
GENERIC_ACTIONS_KEY = 'actions'
GENERIC_ACTION_TAG_KEY = TAG_KEY
GENERIC_ACTION_COST_KEY = LABEL_COST_KEY
GENERIC_ACTION_PROBABILITY_KEY = LABEL_PROBABILITY_KEY
GENERIC_ACTION_SELECTED_KEY = 'selected'
GENERIC_ACTION_FEATURES_KEY = 'action_features'
GENERIC_DEFAULT_SHARED_FEATURES_NAMESAPCE = 'f'
GENERIC_DEFAULT_ACTION_FEATURES_NAMESAPCE = 'j'
GENERIC_EVENT_ID_KEY = EVENT_ID_KEY
GENERIC_TIMESTAMP_KEY = TIMESTAMP_KEY


DSJSON_LABEL_COST_KEY = LABEL_COST_KEY
DSJSON_LABEL_PROBABILITY_KEY = LABEL_PROBABILITY_KEY
DSJSON_LABEL_ACTION_KEY = LABEL_ACTION_KEY
DSJSON_LABEL_INDEX_KEY = LABEL_INDEX_KEY
DSJSON_ACTION_TAG_KEY = TAG_KEY
DSJSON_ACTIONS_KEY = 'a'
DSJSON_CONTENT_KEY = 'c'
DSJSON_DEFAULT_SHARED_NAMESPACE_KEY = 'f'
DSJSON_ACTION_ID_NAMESPACE = 'i'
DSJSON_ACTION_FEATURES_NAMESPACE = 'j'
DSJSON_ACTION_ID_KEY = 'id'
DSJSON_PROBABILITIES_KEY = 'p'
DSJSON_EVENT_ID_KEY = EVENT_ID_KEY
DSJSON_TIMESTAMP_KEY = TIMESTAMP_KEY

DSJSON_DEFAULT_LABEL_COST = 0.0
DSJSON_DEFAULT_LABEL_PROBABILITY = 0.0
DSJSON_DEFAULT_LABEL_ACTION = 1
DSJSON_DEFAULT_LABEL_INDEX = 0

VW_SHARED_FEATURES_KEY = 'shared'

REQUEST_CONTEXT_FEATURES_KEY =  'context_features'
REQUEST_ACTIONS_KEY = GENERIC_ACTIONS_KEY
REQUEST_ACTION_ID_KEY = 'id'
REQUEST_ACTION_TAG_KEY = TAG_KEY
REQUEST_FEATURES_KEY = 'features'
REQUEST_ACTIONS_KEY = 'actions'
REQUEST_EVENT_ID_KEY = 'eventId'
REQUEST_EXCLUDED_ACTIONS_KEY = 'excludedActions'
REQUEST_DEFER_ACTIVATION_KEY = 'deferActivation'


class Sample:
    """
    Holds a single sample from a contextual bandit(CB) dataset in JSON readable format.
       Offers ability to construct a new sample or to input/output into Vowpal Wabbit (vw) or DSJSON format.
       NOTE: support for vw format is very basic. Single and multi-line format is supported but only for
       data files with no complex nested namespaces (root level shared features and _multi action array). 
       Even as such vw-format input/output should be treated with caution.
    
    TODO:
        1. Convert to tabular format and to a pandas DF.
    """
    
    def __init__(self, data = None):
        """
        Constructor, builds empty sample or optionally initializes from a json-readable dict.
        
        Keyword Arguments:
            data {dict} -- json-readable dict to initialize (default: {None})
        """
        if data == None:
            self.data = {}
        else:
            self.data = data


    def __create_generic_template(self):
        """
        Private method to create main elements of a sample in generic representation
        """
        if self.data:
            return

        self.data[GENERIC_SHARED_FEATURES_KEY] = {}
        self.data[GENERIC_ACTIONS_KEY] = []
        self.data[GENERIC_EVENT_ID_KEY] = uuid.uuid4().hex
        local_time = datetime.now().astimezone()
        self.data[GENERIC_TIMESTAMP_KEY] = local_time.isoformat()


    def __create_dsjson_template(self):
        """
        Private method to create main elements of a dsjson sample
        """
        dsjson = OrderedDict()
        dsjson[DSJSON_LABEL_COST_KEY] = DSJSON_DEFAULT_LABEL_COST
        dsjson[DSJSON_LABEL_PROBABILITY_KEY] = DSJSON_DEFAULT_LABEL_PROBABILITY
        dsjson[DSJSON_LABEL_ACTION_KEY] = DSJSON_DEFAULT_LABEL_ACTION
        dsjson[DSJSON_LABEL_INDEX_KEY] = DSJSON_DEFAULT_LABEL_INDEX
        dsjson[DSJSON_ACTIONS_KEY] = []
        dsjson[DSJSON_CONTENT_KEY] = {}
        dsjson[DSJSON_PROBABILITIES_KEY] = []
        dsjson[DSJSON_EVENT_ID_KEY] = ''
        dsjson[DSJSON_TIMESTAMP_KEY] = ''

        return dsjson

    def add_event_id(self, eventid):
        """
        Add event identifier to the sample.
        
        Arguments:
            eventid {string} -- a string used to identify the sample uniquely
        """
        self.data[GENERIC_EVENT_ID_KEY] = eventid

    def add_shared_features(self, features, namespace=None):
        """
        Adds a group of shared feature (context) in free form. Overwrites values of existing features.

        Arguments:
            features {dict or list} -- a dictionary or list of shared features
        """
        self.__create_generic_template()

        if namespace == None:
            namespace = DSJSON_DEFAULT_SHARED_NAMESPACE_KEY

        shared_features = self.data[GENERIC_SHARED_FEATURES_KEY]
        if namespace in shared_features:
            if isinstance(shared_features[namespace], list) and isinstance(features, list):
                shared_features[namespace] += features
            elif isinstance(shared_features[namespace], dict) and isinstance(features, dict):
                shared_features[namespace] = {**shared_features[namespace], **features}
            else:
                raise ValueError("Incompatible data types.")
        else:
            shared_features[namespace] = features

        
    def add_shared_feature(self, feature_name, feature_value, namespace=None):
        """
        Adds (or replaces if already exists) a single shared feature (context).
        
        Arguments:
            feature_name {string} -- the name of the shared feature
            feature_value {string} -- the value fo the shared feature
            namespace {string} --
        """
        self.__create_generic_template()

        if namespace == None:
            namespace = GENERIC_DEFAULT_SHARED_FEATURES_NAMESAPCE

        shared_features = self.data[GENERIC_SHARED_FEATURES_KEY]
        if namespace not in shared_features:
            shared_features[namespace] = {}

        shared_features_of_namespace = self.data[GENERIC_SHARED_FEATURES_KEY][namespace]
        if isinstance(shared_features_of_namespace, dict):
            shared_features_of_namespace[feature_name] = feature_value
        elif isinstance(shared_features_of_namespace, list):
            if not shared_features_of_namespace: # if list of shared features under this namespace is empty
                shared_features.append({})
            shared_features[0][feature_name] = feature_value
        else:
            raise NotImplementedError('Adding shrared feature is not implemented for structures other list or dict.')


    def add_action(self, features, cost=None, probability=None, action_tag=None, selected=False):
        """
        Adds a single action's to the set of actions in the CB sample.
           Though not strictly required by dsjson, an id field is required for APS requests
           so if not provided it will be added by default. All other features will be stored in
           a 'features' key.
        
        Arguments:
            features {dict} -- dict of key-value pairs describing the action's features
            cost {float} -- The cost of the action (reward * -1)
            probability {float} -- The probability of having chosen this action.
        
        Keyword Arguments:
            action_tag {string} -- '_tag' feature of the action that was selected (default: {None})
        """
        self.__create_generic_template()

        actions = self.data[GENERIC_ACTIONS_KEY]
        actions.append({})
        action_index = len(actions) - 1
        if action_tag == None:
            action_tag = str(action_index + 1)

        actions[action_index][GENERIC_ACTION_TAG_KEY] = action_tag
        actions[action_index][GENERIC_ACTION_COST_KEY] = cost
        actions[action_index][GENERIC_ACTION_PROBABILITY_KEY] = probability
        actions[action_index][GENERIC_ACTION_SELECTED_KEY] = selected
        if isinstance(features, list):
            actions[action_index][GENERIC_ACTION_FEATURES_KEY] = features
        else:
            actions[action_index][GENERIC_ACTION_FEATURES_KEY] = [features]
        
        return action_index

    
    def add_label(self, cost, probability, action_tag=None, action_index=None, selected=True):
        """
        Adds information about the chosen action to enable counterfactual estimation or training.
        
        Arguments:
            cost {float} -- The cost of the action (reward * -1)
            probability {float} -- The probability of having chosen this action.
        
        Keyword Arguments:
            action_tag {string} -- '_tag' feature of the action that was selected (default: {None})
            action_index {int} -- 0-based index in the action array of the action that was selected
                Either action_tag or action_index must be provided. If both are given, action_tag
                is used and action_index is ignored. (default: {None})
        """
        if action_tag == None and action_index == None:
            raise ValueError('When adding label, either action tag or action index must be provided.')

        actions = self.data[GENERIC_ACTIONS_KEY]

        if action_tag == None:
            try:
                actions[action_index]
            except IndexError:
                raise IndexError('The action index passed in is out of range.')
        else:
            try:
                labeled_action = [a for a in actions if GENERIC_ACTION_TAG_KEY in a.keys() and a[GENERIC_ACTION_TAG_KEY] == action_tag][0]
                action_index = actions.index(labeled_action)

            except KeyError:
                raise KeyError('No actions match the tag passed in the label.')

        if action_tag:
            actions[action_index][GENERIC_ACTION_TAG_KEY] = action_tag
        actions[action_index][GENERIC_ACTION_COST_KEY] = cost
        actions[action_index][GENERIC_ACTION_PROBABILITY_KEY] = probability
        actions[action_index][GENERIC_ACTION_SELECTED_KEY] = selected


    def add_property(self, property_name, property_value):
        """
        Adds property_name to Sample.data and sets it to given value. Updates the property if it already exists.

        Arguments:
            property_name { string } -- The name of the property
            property_value { } -- The property value
        """
        self.__create_generic_template()
        self.data[property_name] = property_value


    def __str__(self):
        return json.dumps(self.data, indent=2)

    
    @staticmethod
    def from_dsjson(sample_str):
        """
        Converts a dsjson format CB impression into a Sample instance.

        Arguments:
           sample_str {str} -- json-parsable string representing a single CB impression in dsjson format
        """
        dsjson = json.loads(sample_str)

        sample = Sample()

        if DSJSON_EVENT_ID_KEY in dsjson:
            sample.add_property(GENERIC_EVENT_ID_KEY, dsjson[DSJSON_EVENT_ID_KEY])

        if DSJSON_TIMESTAMP_KEY in dsjson:
            sample.add_property(GENERIC_TIMESTAMP_KEY, dsjson[DSJSON_TIMESTAMP_KEY])

        content = dsjson[DSJSON_CONTENT_KEY]
        for namespace, feature_group in content.items():
            if namespace.startswith(MULTI_KEY):
                continue
            sample.add_shared_features(feature_group, namespace)
        
        actions = dsjson[DSJSON_CONTENT_KEY][MULTI_KEY]

        selected_action_index = dsjson[DSJSON_LABEL_INDEX_KEY]
        selected_action_cost = dsjson[DSJSON_LABEL_COST_KEY]
        selected_action_probability = dsjson[DSJSON_LABEL_PROBABILITY_KEY]

        for action_index, action in enumerate(actions):
            features = action[DSJSON_ACTION_FEATURES_NAMESPACE]
            tag = action[DSJSON_ACTION_TAG_KEY]
            if action_index == selected_action_index:
                sample.add_action(features, selected_action_cost, selected_action_probability, tag, selected=True)
            else:
                try:
                    action_index_index = dsjson[DSJSON_ACTIONS_KEY].index(action_index + 1)
                    action_probability = dsjson[DSJSON_PROBABILITIES_KEY][action_index_index]
                    sample.add_action(features, probability = action_probability, action_tag = tag)
                except ValueError:
                    sample.add_action(features, action_tag = tag)

        return sample


    def to_dsjson(self, indent=None):
        """
        Converts example to dsjson format string
        
        Keyword Arguments:
            indent {int} -- indenting will force pretty print of the json string (default: {None})
        """
        dsjson = self.__create_dsjson_template()
        
        # process misc properties
        dsjson[DSJSON_EVENT_ID_KEY] = self.data[GENERIC_EVENT_ID_KEY]
        dsjson[DSJSON_TIMESTAMP_KEY] = self.data[GENERIC_TIMESTAMP_KEY]

        # process shared features
        shared_features = self.data[GENERIC_SHARED_FEATURES_KEY]
        for namespace, features in shared_features.items():
            dsjson[DSJSON_CONTENT_KEY][namespace] = features

        # process actions
        actions = self.data[GENERIC_ACTIONS_KEY]
        
        if len(actions) > 0:
            dsjson[DSJSON_CONTENT_KEY][MULTI_KEY] = []
        
        action_index = 0
        for action in actions:
            dsjson[DSJSON_CONTENT_KEY][MULTI_KEY].append({})
            dsjson_action = dsjson[DSJSON_CONTENT_KEY][MULTI_KEY][action_index]
            dsjson_action[DSJSON_ACTION_TAG_KEY] = action[GENERIC_ACTION_TAG_KEY]
            dsjson_action[DSJSON_ACTION_ID_NAMESPACE] = {}
            dsjson_action[DSJSON_ACTION_ID_NAMESPACE][DSJSON_ACTION_ID_KEY] = action[GENERIC_ACTION_TAG_KEY]
            dsjson_action[DSJSON_ACTION_FEATURES_NAMESPACE] = action[GENERIC_ACTION_FEATURES_KEY]

            action_probability = action[GENERIC_ACTION_PROBABILITY_KEY]
            if action[GENERIC_ACTION_SELECTED_KEY] == True:
                dsjson[DSJSON_LABEL_PROBABILITY_KEY] = action_probability
                dsjson[DSJSON_LABEL_COST_KEY] = action[GENERIC_ACTION_COST_KEY]
                dsjson[DSJSON_LABEL_ACTION_KEY] = action_index + 1
                dsjson[DSJSON_LABEL_INDEX_KEY] = action_index

            # rearrange action indicies and corresponding probabilities 
            if action_probability:
                action_indices = dsjson[DSJSON_ACTIONS_KEY]
                probabilities =dsjson[DSJSON_PROBABILITIES_KEY]

                action_indices.append(action_index + 1)
                probabilities.append(action_probability)

                i = len(action_indices) - 1
                while i > 0 and probabilities[i] > probabilities[i - 1]:
                    temp = probabilities[i]
                    probabilities[i] = probabilities[i - 1]
                    probabilities[i - 1] = temp
                    temp = action_indices[i]
                    action_indices[i] = action_indices[i - 1]
                    action_indices[i - 1] = temp
                    i = i - 1

            action_index = action_index + 1

        dsjson_str = json.dumps(dsjson, indent=indent, separators=(',', ':')) + '\n'

        return dsjson_str


    @staticmethod
    def _parse_vw_features(features_str, default_namespace):
        feature_groups = {}
        rest = features_str
        head = ''
        while True:
            head, _, rest = rest.partition('|')
            if not head:
                break

            namespace = default_namespace
            if not head.startswith(' '):
                namespace, _, head = head.partition(' ')
            if namespace not in feature_groups:
                feature_groups[namespace] = {}

            features = head.split()
            for feature in features:
                if ':' in feature:
                    key, value = feature.split(':')
                    feature_groups[namespace][key] = float(value)
                else:
                    feature_groups[namespace][feature] = 1

        return feature_groups


    @staticmethod
    def _parse_vw_label(label_str):
        tag = None
        action_cost = None
        action_probability = None

        if ':' in label_str:
            label_items = label_str.split(':')
            num_items = len(label_items)
            if num_items <= 3:
                if num_items == 3:
                    tag = label_items[0]
                action_cost = float(label_items[num_items - 2])
                action_probability = float(label_items[num_items - 1])
            else:
                raise ValueError('Invalid label: {}'.format(label_str))
        else:
            tag = label_str

        return (tag, action_cost, action_probability)


    @staticmethod
    def from_vw(sample_str):
        """
        Converts a vw format CB impression into a Sample instance. There is no error checking,
        so the assumption is made that the format provided is valid.

        Arguments:
           sample_str {str} -- vw-formatted string representing a single CB impression
        """

        #TODO: error handling for invalid format

        sample = Sample()
        sample_str = sample_str.strip()
        if sample_str.startswith(VW_SHARED_FEATURES_KEY):
            #multi-line format
            rows = sample_str.split('\n')
            for row in rows:
                front, _, back = row.partition('|')
                front = front.strip()
                if front == VW_SHARED_FEATURES_KEY:
                    shared_features = Sample._parse_vw_features(back, GENERIC_DEFAULT_SHARED_FEATURES_NAMESAPCE)
                    for namespace, feature_group in shared_features.items():
                        for key, value in feature_group.items():
                            sample.add_shared_feature(key, value, namespace)
                else:
                    tag, cost, probability = Sample._parse_vw_label(front)
                    action_features = Sample._parse_vw_features(back, GENERIC_DEFAULT_ACTION_FEATURES_NAMESAPCE)
                    selected = False
                    if cost and probability:
                        selected = True
                    sample.add_action(action_features, cost, probability, tag, selected)
        else:
            # single-line format
            front, _, back = sample_str.partition('|')
            front = front.strip()
            tag, cost, probability = Sample._parse_vw_label(front)
            action_features = Sample._parse_vw_features(back, GENERIC_DEFAULT_ACTION_FEATURES_NAMESAPCE)
            selected = False
            if cost and probability:
                selected = True
            sample.add_action(action_features, cost, probability, tag, selected)

        return sample


    def to_vw(self):
        """
        Converts the sample to vw format string. Currently only works with non-nested namespaces
           (shared features must be in root namespace and actions in the _multi array)
        """

        sample_str = ''

        context_str = ''
        if GENERIC_SHARED_FEATURES_KEY in self.data:
            context_str = self._features_to_str(self.data[GENERIC_SHARED_FEATURES_KEY], GENERIC_DEFAULT_SHARED_FEATURES_NAMESAPCE)

        if context_str:
            sample_str = f'{VW_SHARED_FEATURES_KEY} {context_str}\n'

        if GENERIC_ACTIONS_KEY in self.data:
            for action in self.data[GENERIC_ACTIONS_KEY]:
                label_str = ''
                if action[GENERIC_ACTION_TAG_KEY]:
                    label_str = f'{action[GENERIC_ACTION_TAG_KEY]}'

                if action[GENERIC_ACTION_SELECTED_KEY] == True:
                    if not label_str:
                        label_str = '0'
                    label_str += f':{action[GENERIC_ACTION_COST_KEY]}:{action[GENERIC_ACTION_PROBABILITY_KEY]}'

                if label_str:
                    label_str += ' '

                action_features_str = self._features_to_str(action[GENERIC_ACTION_FEATURES_KEY], GENERIC_DEFAULT_ACTION_FEATURES_NAMESAPCE)

                sample_str += f'{label_str}{action_features_str}\n'
        
        if context_str:
            sample_str += '\n'
        
        return sample_str


    def to_aps_request(self, event_id, excluded_actions = [], defer_activation = False):
        """
        Converts sample to a json serializable dict that can be sent in a POST request to Personalizer.

        Arguments:
            event_id {int} -- Event id to pass to personalizer
        
        Keyword Arguments:
            excluded_actions {array} -- array of excluded actions (default: [])
            defer_activation {bool} -- defer activation flag (default: False)
        """
        aps_json = {}
        aps_json[REQUEST_CONTEXT_FEATURES_KEY] = self.data[GENERIC_SHARED_FEATURES_KEY] # TODO: This may not work
        aps_json[REQUEST_ACTIONS_KEY] = []
        for action in self.data[GENERIC_ACTIONS_KEY]:
            aps_action = {}
            aps_action[REQUEST_ACTION_ID_KEY] = action[GENERIC_ACTION_TAG_KEY] # TODO: Figure this out
            aps_action[REQUEST_ACTION_TAG_KEY] = action[GENERIC_ACTION_TAG_KEY]
            aps_action[REQUEST_FEATURES_KEY] = action[GENERIC_ACTION_FEATURES_KEY]
            aps_json[REQUEST_ACTIONS_KEY].append(aps_action)
        aps_json[REQUEST_EVENT_ID_KEY] = event_id
        aps_json[REQUEST_EXCLUDED_ACTIONS_KEY] = excluded_actions
        aps_json[REQUEST_DEFER_ACTIVATION_KEY] = defer_activation
    
        return aps_json


    @staticmethod
    def _join_features_strs(str1, str2):
        if str1 and str2:
            str1 += ' '
        return str1 + str2


    @staticmethod
    def _features_to_str(features, namespace):
        features_str = ''
        if len(features) == 0:
            return features_str

        if namespace.startswith('_'): # TODO: We may want to preserve _text, _tag, etc.
            return features_str

        complex_feature_str = ''
        if type(features) is list:
            for item in features:
                returned_feature_str = Sample._features_to_str(item, namespace)
                complex_feature_str = Sample._join_features_strs(complex_feature_str, returned_feature_str)

        elif type(features) is dict:
            for key, value in features.items():
                if isinstance(value, (list, dict)):
                    returned_feature_str = Sample._features_to_str(value, key)
                    complex_feature_str = Sample._join_features_strs(complex_feature_str, returned_feature_str)
                else:
                    if not features_str:
                        features_str = f'|{namespace}'
                    features_str += Sample._feature_to_str(key, value)

        features_str = Sample._join_features_strs(features_str, complex_feature_str)
        return features_str


    @staticmethod
    def _feature_to_str(feature, value):
        if isinstance(value, str):
            return f' {feature}={value}'
        else:
            return f' {feature}:{value}'
