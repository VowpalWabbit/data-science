import ccb.dsjson.filters as filters
import json
from collections import ChainMap

# +
def json_load(line):
    try:
        return json.loads(line)
    except:
        return json.loads(line.encode('utf-8','ignore'))

class Processor:
    default_top_processor = lambda o: {
        'Timestamp': o['Timestamp'],
        '_skipLearn': o.get('_skipLearn', False),
        'pdrop': o.get('pdrop', 0.0)}
    
    default_slot_processor = lambda o: {
        '_inc': o.get('_inc', [])}
    
    default_outcome_processor = lambda o: {
        '_label_cost': o['_label_cost'],
        '_id': o['_id'],
        '_a': o['_a'],
        '_p': o['_p']} 
    
    processors = {
        '/': [default_top_processor],
        'c': [],
        '_multi': [],
        '_slots': [default_slot_processor],
        '_outcomes': [default_outcome_processor]
    }

    filters = [filters.is_decision]

    def __init__(self, processors = None, filters = None):
        self.processors = processors if processors is not None else self.processors
        self.filters = filters if filters is not None else self.filters

    def _process_decision(self, line):
        parsed = json_load(line)
        top = dict(ChainMap(*[p(parsed) for p in self.processors['/']]))
        shared = dict(ChainMap(*[p(parsed['c']) for p in self.processors['c']]))

        actions = [None] * len(parsed['c']['_multi'])
        for i, o in enumerate(parsed['c']['_multi']):
            actions[i] = dict(ChainMap(*[p(o) for p in self.processors['_multi']]))

        slots = [None] * len(parsed['c']['_slots'])
        for i, o in enumerate(parsed['c']['_slots']):
            slots[i] = dict(ChainMap(*[p(o) for p in self.processors['_slots']]))
        
        outcomes = [None] * len(parsed['_outcomes'])
        for i, o in enumerate(parsed['_outcomes']):
            outcomes[i] = dict(ChainMap(*[p(o) for p in self.processors['_outcomes']]))
        
        result = dict(ChainMap(top, {'c': dict(ChainMap(shared, {'_multi': actions, '_slots': slots})), '_outcomes': outcomes}))
        return json.dumps(result)

    def process(self, lines):
        for f in self.filters:
            lines = filter(lambda l: f(l), lines)
        if len(self.processors) > 1:
            return map(lambda l: f'{self._process_decision(l)}\n', lines) 
        return lines
