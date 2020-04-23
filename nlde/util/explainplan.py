#!/usr/bin/env python

"""
Created on August 20, 2018

@author: Lars Heling
@author: Maribel Acosta
"""
from nlde.planner.optimizer import TreePlan, IndependentOperator, DependentOperator
import json


def explain_plan(tree):
    s = {
            'id': hash(tree),
            'label': list(tree.vars),
            'left': operator_tree_to_json(tree.left),
            'right': operator_tree_to_json(tree.right),
            'type': tree.operator.__class__.__name__,
            'card': tree.total_res
    }

    print json.dumps(s)
    #with open("/tmp/tree.json", 'w') as tree_file:
    #    json.dump(s, tree_file)


def operator_tree_to_json(node):
    if type(node) == TreePlan:
        s = {
            'id': hash(node),
            'label': list(node.vars),
            'left': operator_tree_to_json(node.left),
            'right': operator_tree_to_json(node.right),
            'type': node.operator.__class__.__name__,
            'card': node.total_res
        }
        return s

    elif type(node) == IndependentOperator or type(node) == DependentOperator:
        return {
            'id': hash(node),
            'label':  str(node.query),
            'source':  str(node.server),
            'type': node.__class__.__name__,
            'card': node.total_res,
            'source': node.server
        }

    elif node is None:
        return None

    else:
        return str(node)