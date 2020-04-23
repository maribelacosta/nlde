'''
Generates a plan for the query in query/motex3.rq
'''
from nlde.engine.contactsource import get_metadata_ldf
from nlde.operators.fjoin import Fjoin
from nlde.util.sparqlparser import parse
from nlde.planner.optimizer import IndependentOperator, TreePlan


def create_plan(query, eddies, source):

    # Plan structures.
    tree_height = 0
    id_operator = 0
    operators = []
    operators_desc = {}
    plan_order = {}
    operators_vars = {}
    ordered_subtrees = []
    independent_sources = 0
    eofs_operators_desc = {}
    operators_sym = {}
    sources_desc = {}
    eofs_desc = None
    subtrees = []

    # Create initial signatures and leaves of the plan.
    for subquery in query.where.left.triple_patterns:
        sources_desc.update({id_operator: 0})
        leaf = IndependentOperator(id_operator, source, subquery, sources_desc, subquery.get_variables (), eddies, sources_desc)
        leaf.total_res = get_metadata_ldf(leaf.server, leaf.query)
        subtrees.append(leaf)
        ordered_subtrees.append(leaf.total_res)
        id_operator += 1

    # Now we are going to join the first two triple patterns in the query.

    ## CHANGE THESE LINES
    id_operator = 0
    left = subtrees[0]
    right = subtrees[1]

    join_variables = set(left.vars) & set(right.vars)
    all_variables = set(left.vars) | set(right.vars)
    sources = {}
    sources.update(left.sources)
    sources.update(right.sources)
    res = (left.total_res + right.total_res) / 2
    operators_desc[id_operator] = {}

    ## CHANGE THESE LINES
    operators_desc[id_operator].update({list(left.sources)[0]: 0})
    operators_desc[id_operator].update({list(right.sources)[0]: 1})
    sources_desc[list(left.sources)[0]] = 3 # The ready is 11
    sources_desc[list(right.sources)[0]] = 3 # The ready is 11


    op = Fjoin(id_operator, join_variables, eddies)
    operators.append(op)
    # Example here of an array of left plans.
    tree = TreePlan(op, all_variables, join_variables, sources, [left], right, tree_height, res)
    operators_vars[id_operator] = join_variables
    independent_sources = independent_sources + 2
    operators_sym.update({id_operator: True})
    plan_order[id_operator] = tree_height
    tree_height = tree_height + 1

    # Now add the third triple pattern to the plan.

    ## CHANGE THESE LINES
    id_operator = 1
    left = tree
    right = subtrees[2]

    join_variables = set(left.vars) & set(right.vars)
    all_variables = set(left.vars) | set(right.vars)
    sources = {}
    sources.update(left.sources)
    sources.update(right.sources)
    res = (left.total_res + right.total_res) / 2
    operators_desc[id_operator] = {}

    ## CHANGE THESE LINES
    operators_desc[id_operator].update({list(left.sources)[0]: 0, list(left.sources)[1]: 0})
    operators_desc[id_operator].update({list(right.sources)[0]: 1})
    sources_desc[list(right.sources)[0]] = 2 # The ready is 10

    op = Fjoin(id_operator, join_variables, eddies)
    operators.append(op)
    tree = TreePlan(op, all_variables, join_variables, sources, left, right, tree_height, res)
    operators_vars[id_operator] = join_variables
    independent_sources = independent_sources + 1
    operators_sym.update ({id_operator: True})
    plan_order[id_operator] = tree_height
    tree_height = tree_height + 1

    return tree, tree.height, operators_desc, sources_desc, plan_order, operators_vars, independent_sources, operators_desc, operators_sym, operators



if __name__ == '__main__':

    # The triple patterns in query should be ordered already.
    query = "/home/mac/queries/motex.rq"
    source = "http://172.22.204.207:3000/db"
    queryparsed = parse(open(query).read())

    (tree,
     tree_height,
     operators_desc,
     sources_desc,
     plan_order,
     operators_vars,
     independent_sources,
     eofs_operators_desc,
     operators_sym,
     operators) = create_plan(queryparsed, 2, source)

    print "Plan"
    print tree
