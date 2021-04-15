import json

def line_2_slot(line):
    return {int(p.split(':')[0]) : float(p.split(':')[1])  for p in line.split(',')}

def lines_2_slots(lines):
    result = []
    for l in lines:
        if not l.isspace():
            result.append(line_2_slot(l))
        else:
            yield result
            result = []

def lines_2_predictions(vw_predictions, pipeline_predictions, policy_name):
    stream = zip(map(lambda l: json.loads(l), pipeline_predictions), lines_2_slots(vw_predictions))
    for dp in stream:
        decision = dp[0]
        prediction = dp[1]
        t = decision['i']['t']
        a = decision['a']
        r = decision['r']
        p = decision['p']
        n = decision['n']
        yield {'i': {'t': t}, 'a': a, 'r': r, 'p': p, 'n': n, 
            'b': { policy_name: [prediction[i][a[i]] for i in range(len(prediction))]}}