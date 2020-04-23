from multiprocessing import Process, Queue
from nlde.engine.contactsource import get_metadata_ldf, contact_ldf_server
from nlde.operators.operatorstructures import Tuple
from nlde.operators.fjoin import Fjoin
from nlde.operators.xdistinct import Xdistinct
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.xproject import Xproject
from nlde.util.querystructures import TriplePattern, Argument
from nlde.util.sparqlparser import parse
from nlde.planner.optimizer import IndependentOperator, TreePlan


def create_plan(query, eddies, server):

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
        eofs_desc.update({id_operator: 0})
        leaf = IndependentOperator(id_operator, source, subquery, sources_desc, subquery.get_variables (), eddies, eofs_desc)
        leaf.total_res = get_metadata_ldf(leaf.server, leaf.query)
        subtrees.append(leaf)
        ordered_subtrees.append(leaf.total_res)
        id_operator += 1

    # Now we are going to create a plan with a Join.
    id_operator = 0
    left = subtrees[0]
    right = subtrees[1]
    join_variables = set(left) & set(right.join_vars)
    all_variables = set(left.vars) | set(right.vars)
    sources = {}
    sources.update(left.sources)
    sources.update(right.sources)
    res = left.total_res + right.total_res

    operators_desc[id_operator] = {}
    operators_desc[id_operator].update({left.sources[0]: -1})
    operators_desc[id_operator].update ({right.sources[0]: 1})

    op = Fjoin(id_operator, join_variables, eddies)
    operators.append (op)
    star_tree = TreePlan(op, all_variables, join_variables, sources, left, right, 1, res)
    independent_sources = independent_sources + 1
    operators_sym.update ({id_operator: True})

    id_operator = 1

    return tree, tree.height, operators_desc, sources_desc, plan_order, operators_vars, independent_sources, operators_desc, operators_sym, operators



if __name__ == '__main__':

    # The triple patterns in query should be ordered already.
    query = "path"
    source = "pathtoserver"
    queryparsed = parse(query)

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


