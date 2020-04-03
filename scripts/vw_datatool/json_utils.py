import json

   
def parse_route(data, route):
    node = data
    rest = route
    head = ''
    while True:
        if '|' not in rest:
            head = rest
            rest = ''
        else:
            head, rest = rest.split('|', 1)
        if not head:
            break

        head_name = head
        head_index = ''
        if '[' in head_name:
            head_name, head_index = head.split('[')
        if head_name not in node:
            raise ValueError('Invalid namespace route at {}.'.format(head_name))
        index = -1
        if head_index:
            index = int(head_index.strip()[:-1])
        if index < 0:
            node = node[head_name]
        else:
            node = node[head_name][index]

    return node


def add_list(data, route, list_name):
    """
    Adds a new feature list under the given route.

    Arguments:
        data {} -- 
        route {string} -- path to the node where the new list will be created. It must be a valid path.
            example: "c|d"
        list_name { string } -- name of the new list
    """
    node = parse_route(data, route)
    node[list_name] = []

    new_node = node[list_name]
    new_node_route = route + '|' + list_name

    return (new_node, new_node_route)


def add_container(data, route, container_name):
    """
    Adds ...

    Arguments:
        data {} --
        route {string} -- path to the node where the new namespace will be created. It must be a valid path.
            example: "c|_multi[1]"
        container_name {string} -- the name of the feature
    """
    node = parse_route(data, route)
    node[container_name] = {}

    new_node = node[container_name]
    new_node_route = route + '|' + container_name

    return (new_node, new_node_route)


def append_element(data, route, element=None):
    """
    Appends a new element
    
    Arguments:
        data {} -- 
        route {string} -- path to the node where the new feature list will be created. It must be a valid path.
            example: "c|_multi"
        element --
    """
    node = parse_route(data, route)
    if element:
        node.append(element)
    else:
        node.append({})

    new_node_index = len(node) - 1
    new_node = node[new_node_index]
    new_node_route = route + '[' + str(new_node_index) + ']'

    return (new_node, new_node_route, new_node_index)


def set_element(data, route, element_name, element_value=None):
    """
    Adds (or replaces if already exists) a single element.

    Arguments:
        data {} --
        route {string} -- path to the node where the new namespace will be created. It must be a valid path.
            example: "c|_multi[1]"
        element_name {string} -- the name of the feature
        element_value {string, integer, float, boolean, ...} -- the value fo the feature
    """
    node = parse_route(data, route)
    node[element_name] = element_value

    new_node = node[element_name]
    new_node_route = route + '|' + element_name

    return (new_node, new_node_route)


def get_element(data, route):
    """
    Returns the node ...

    Arguments:
        data {} --
        route {string} -- path to the node where the new namespace will be created. It must be a valid path.
            example: "c|_multi[1]"
    """
    node = parse_route(data, route)
    return (node, route)

