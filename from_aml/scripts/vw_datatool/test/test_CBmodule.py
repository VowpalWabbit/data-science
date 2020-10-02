# +
# TODO: convert these into proper unit and/or e2e tests

import json
from CBSample import Sample
import CBIO
# -

# multi-action
# create an array of CB impressions
ma_samples = []

# first sample
s = Sample()

s.add_shared_feature("username", "billy")
s.add_shared_feature("time_of_day", "morning")
s.add_shared_feature("a", 1.2)

s.add_action({"type":"hot","origin":"kenya","organic":"yes","roast":"dark"},
    1, 0.15, action_tag="Cappucino", selected=False)
s.add_action({"type":"cold","origin":"brazil","organic":"yes","roast":"light"},
    3, 0.25, action_tag="Coldbrew")
s.add_action({"type":"cold","origin":"ethiopia","organic":"no","roast":"light"},
    0.5, 0.35, action_tag="Icedmocha", selected=True)
s.add_action({"type":"hot","origin":"brazil","organic":"no","roast":"dark"},
    2, 0.25, action_tag="Latte")

ma_samples.append(s)

# second sample
s = Sample()

s.add_shared_features([{"username": {"firstname":"hossein", "lastname":"azari"}}])
s.add_shared_features([{"time_of_day": "morning", "season": "winter"}])

s.add_action({"type":"hot","origin":"kenya","organic":"yes","roast":"dark"}, action_tag="Cappucino", selected=False)
s.add_action({"type":"cold","origin":"brazil","organic":"yes","roast":"light"},
    3, 0.25, action_tag="Coldbrew")
s.add_action({"type":"hot","origin":"brazil","organic":"no","roast":"dark"},
    2, 0.35, action_tag="Latte", selected=True)

ma_samples.append(s)

# print to screen
print(s.to_dsjson(2))

dsjson_str = s.to_dsjson()
s2 = Sample.from_dsjson(dsjson_str)
print('\nConverted from dsjson string:\n')
print(s2)

# third sample
s = Sample()
s.add_shared_feature("username", "joe")
s.add_shared_feature("time_of_day", "evening")
s.add_shared_feature("a", 3.4)
action_index = s.add_action({"type":"hot","origin":"kenya","organic":"yes","roast":"dark"}, selected=True)
s.add_label(3.5, 0.2, action_index=action_index)

ma_samples.append(s)

# save to json and vw files
CBIO.samples_to_file(ma_samples, 'test.json')
CBIO.samples_to_file(ma_samples, 'test.txt', format='vw')

# convert to APS request
for sample in ma_samples:
    print(sample.to_aps_request(1))

# import export dsjson
ma_samples = CBIO.samples_from_file('test.json')
CBIO.samples_to_file(ma_samples, 'test1.json')

# import export vw (assume no nested namespaces)
ma_samples = CBIO.samples_from_file('test.txt', format='vw')
CBIO.samples_to_file(ma_samples, 'test1.txt', format='vw')

# convert vw and append to dsjson
ma_samples = CBIO.samples_from_file('test.txt', format='vw')
CBIO.samples_to_file(ma_samples, 'test1.json', format='dsjson', append=True)


# single action samples
# create an array of CB impressions
sa_samples = []

# first sample
s = Sample()
s.add_action({"type":"hot","origin":"kenya","organic":"yes","roast":"dark"}, 1, 0.15, selected=True)
sa_samples.append(s)

# second sample
s = Sample()
s.add_action({"type":"cold","origin":"brazil","organic":"yes","roast":"light"}, 3, 0.25)
sa_samples.append(s)

# print to screen
print(s.to_dsjson(2))

dsjson_str = s.to_dsjson()
s2 = Sample.from_dsjson(dsjson_str)
print('\nConverted from dsjson string:\n')
print(s2)

# third sample
s = Sample()
action_index = s.add_action({"type":"hot","origin":"kenya","organic":"yes","roast":"dark"})
s.add_label(3.5, 0.2, action_index=action_index)
sa_samples.append(s)

# save to json and vw files
CBIO.samples_to_file(sa_samples, 'test_sa.json')
CBIO.samples_to_file(sa_samples, 'test_sa.txt', format='vw')

# convert to APS request
for sample in sa_samples:
    print(sample.to_aps_request(1))

# import export dsjson
sa_samples = CBIO.samples_from_file('test_sa.json')
CBIO.samples_to_file(sa_samples, 'test1_sa.json')

# import export vw (assume no nested namespaces)
sa_samples = CBIO.samples_from_file('test_sa.txt', format='vw')
CBIO.samples_to_file(sa_samples, 'test1_sa.txt', format='vw')

# convert vw and append to dsjson
sa_samples = CBIO.samples_from_file('test_sa.txt', format='vw')
CBIO.samples_to_file(sa_samples, 'test1_sa.json', format='dsjson', append=True)
