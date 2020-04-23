from multiprocessing import Process, Queue
from nlde.engine.contactsource import get_metadata_ldf, contact_ldf_server
from nlde.operators.operatorstructures import Tuple
from nlde.operators.fjoin import Fjoin
from nlde.operators.xdistinct import Xdistinct
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.xproject import Xproject
from nlde.util.querystructures import TriplePattern, Argument
import math

class DependentOperator(object):
    """
    Implements a plan leaf that is resolved by a dependent physical operator.

    The execute() method reads tuples from the input queue and
    place them in the output queue.
    """

    def __init__(self, sources, server, query, sources_desc, variables=None, res=0):

        if variables is None:
            variables = []

        self.server = server
        self.q = Queue()
        self.sources = sources
        self.server = server
        self.query = query
        self.sources_desc = sources_desc
        self.vars = variables
        self.join_vars = variables
        self.total_res = res
        self.height = 0
        self.p = None

    def aux(self, n):
        return " Dependent: ", self.query

    def execute(self, variables, instances, outputqueue, p_list=None):

        # Copy the query array and obtain variables.
        query = [self.query.subject.value, self.query.predicate.value, self.query.object.value]
        variables = list(variables)

        # Instantiate variables in the query.
        inst = {}
        for i in variables:
            inst.update({i: instances[i]})
            inst_aux = str(instances[i])
            for j in (0, 1, 2):
                if query[j] == "?" + i:
                    query[j] = inst_aux

        # TODO: Check if this is redundant.
        for i in range(0, len(query)):
            query[i] = query[i].replace("%%%", " ")

        tp = TriplePattern(Argument(query[0]), Argument(query[1]), Argument(query[2]))

        # Create process to contact source.
        aux_queue = Queue()
        self.p = Process(target=contact_ldf_server, args=(self.server, tp, aux_queue,))
        self.p.start()
        sources = self.sources.keys()

        if p_list:
            p_list.put(self.p.pid)

        # Ready and done vectors.
        ready = self.sources_desc[self.sources.keys()[0]]
        done = 0

        # Get answers from the source.
        data = aux_queue.get(True)
        while data != "EOF":

            # TODO: Check why this is needed.
            data.update(inst)

            # Create tuple and put it in output queue.
            outputqueue.put(Tuple(data, ready, done, sources))

            # Get next answer.
            data = aux_queue.get(True)

        # Close the queue
        aux_queue.close()
        outputqueue.put(Tuple("EOF", ready, done, sources))
        self.p.terminate()


class IndependentOperator(object):
    """
    Implements a plan leaf that can be resolved asynchronously.

    The execute() method reads tuples from the input queue and
    place them in the output queue.
    """

    def __init__(self, sources, server, query, sources_desc, variables=None, eddies=2, eofs_desc={}):
        if variables is None:
            variables = []

        self.server = server
        self.q = Queue()
        self.sources = {sources: variables}
        self.server = server
        self.query = query
        self.sources_desc = sources_desc
        self.vars = variables
        self.join_vars = variables
        self.total_res = -1
        self.height = 0
        self.eddies = eddies
        self.eofs_desc = eofs_desc
        self.p = None

    def aux(self, _):
        return " Independent: " + str(self.query)

    def execute(self, left, right, outputqueues, p_list=None):

        # Determine the output queue.
        if not left:
            outq = right
        else:
            outq = left

        # Contact source.
        self.p = Process(target=contact_ldf_server, args=(self.server, self.query, self.q,))
        self.p.start()

        if p_list:
            p_list.put(self.p.pid)

        # Initialize signature of tuples.
        ready = self.sources_desc[self.sources.keys()[0]]
        done = 0
        sources = self.sources.keys()

        # Read answers from the source.
        data = self.q.get(True)
        count = 0
        while data != "EOF":
            count = count +1
            # Create tuple and put it in the output queue.
            outq.put(Tuple(data, ready, done, sources))
            # Read next answer.
            data = self.q.get(True)

        # Close queue.
        ready_eof = self.eofs_desc[self.sources.keys()[0]]
        outq.put(Tuple("EOF", ready_eof, done, sources))
        outq.close()
        self.p.terminate()


class TreePlan(object):
    """
    Represents a plan to be executed by the engine.

    It is composed by a left node, a right node, and an operators node.
    The left and right nodes can be leaves to contact sources, or subtrees.
    The operators node is a physical operators, provided by the engine.

    The execute() method evaluates the plan.
    It creates a process for every node of the plan.
    The left node is always evaluated.
    If the right node is an independent operators or a subtree, it is evaluated.
    """

    def __init__(self, operator, variables, join_vars, sources, left, right, height=0, res=0):
        self.operator = operator
        self.vars = variables
        self.join_vars = variables
        self.sources = sources
        self.left = left
        self.right = right
        self.height = height
        self.total_res = res
        self.p = None

    def __repr__(self):
        return self.aux(" ")

    def aux(self, n):
        # Node string representation.
        s = n + str(self.operator) + "\n" + n + str(self.vars) + "\n"

        # Left tree plan string representation.
        if self.left:
            s = s + str(self.left.aux(n + "  "))

        # Right tree plan string representation.
        if self.right:
            s = s + str(self.right.aux(n + "  "))

        return s

    def execute(self, operators_input_queues,  eddies_queues, p_list, operators_desc):

        operator_inputs = operators_input_queues[self.operator.id_operator]

        # Execute left sub-plan.
        if self.left:
            # Case: Plan leaf (asynchronous).
            if self.left.__class__.__name__ == "IndependentOperator":
                q = operators_desc[self.operator.id_operator][self.left.sources.keys()[0]]
                p1 = Process(target=self.left.execute,
                             args=(operators_input_queues[self.operator.id_operator][q], None, eddies_queues, p_list,))
                p1.start()
                p_list.put(p1.pid)

            # Case: Tree plan.
            elif self.left.__class__.__name__ == "TreePlan":
                p1 = Process(target=self.left.execute,
                             args=(operators_input_queues, eddies_queues, p_list, operators_desc,))

                p1.start()
                p_list.put(p1.pid)

            # Case: Array of independent operators.
            else:
                for elem in self.left:
                    q = operators_desc[self.operator.id_operator][elem.sources.keys()[0]]
                    p1 = Process(target=elem.execute,
                                 args=(operators_input_queues[self.operator.id_operator][q], None, eddies_queues, p_list,))

                    p1.start()
                    p_list.put(p1.pid)

        # Execute right sub-plan.
        if self.right and ((self.right.__class__.__name__ == "TreePlan") or (self.right.__class__.__name__ == "IndependentOperator")):

            # Case: Plan leaf (asynchronous).
            if self.right.__class__.__name__ == "IndependentOperator":
                q = operators_desc[self.operator.id_operator][self.right.sources.keys()[0]]
                p2 = Process(target=self.right.execute,
                             args=(None, operators_input_queues[self.operator.id_operator][q], eddies_queues, p_list,))
            # Case: Tree plan.
            else:
                p2 = Process(target=self.right.execute,
                             args=(operators_input_queues, eddies_queues, p_list, operators_desc,))

            p2.start()
            p_list.put(p2.pid)
            #right = operators_input_queues[self.operator.id_operator]

        # Right sub-plan. Case: Plan leaf (dependent).
        else:
        # TODO: Change this. Uncomment line below
        #elif self.right:
            operator_inputs = operator_inputs + [self.right]
            #right = self.right

        # Create a process for the operator node.
        self.p = Process(target=self.operator.execute,
                         args=(operator_inputs, eddies_queues,))

        self.p.start()
        p_list.put(self.p.pid)


def estimate_card(l, r):

    # Contraharmonic mean
    return math.sqrt(((l*l) + (r*r))/2.0)


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
    eofs_desc = {}
    subtrees = []

    # Create initial signatures and leaves of the plan.
    for subquery in query.where.left.triple_patterns:
        sources_desc.update({id_operator: 0})
        eofs_desc.update({id_operator: 0})
        leaf = IndependentOperator(id_operator, source, subquery, sources_desc, subquery.get_variables(), eddies, eofs_desc)
        leaf.total_res = get_metadata_ldf(leaf.server, leaf.query)
        subtrees.append(leaf)
        ordered_subtrees.append(leaf.total_res)
        id_operator += 1

    # Order leaves depending on the cardinality of fragments.
    keydict = dict(zip(subtrees, ordered_subtrees))
    subtrees.sort(key=keydict.get)

    # Stage 1: Generate left-linear index nested stars.
    stars = []
    id_operator = 0
    while len(subtrees) > 0:

        to_delete = []
        star_tree = subtrees.pop(0)
        star_vars = star_tree.vars
        tree_height = 0
        independent_sources = independent_sources + 1

        for j in range(0, len(subtrees)):
            subtree_j = subtrees[j]
            join_variables = set(star_vars) & set(subtree_j.join_vars)
            all_variables = set(star_tree.vars) | set(subtree_j.vars)

            # Case: There is a join.
            if len(join_variables) > 0:

                to_delete.append(subtree_j)

                # Update signatures.
                sources = {}
                sources.update(star_tree.sources)
                sources.update(subtree_j.sources)
                operators_desc[id_operator] = {}
                operators_vars[id_operator] = join_variables
                eofs_operators_desc[id_operator] = {}

                # The current tree is the left argument of the plan.
                for source in star_tree.sources.keys():
                    if len(set(sources[source]) & join_variables) > 0:
                        # TODO: Change the next 0 for len of something
                        operators_desc[id_operator].update({source: 0})
                    sources_desc[source] = sources_desc[source] | pow(2, id_operator)

                    # TODO: check this.
                    eofs_operators_desc[id_operator].update({source: 0})
                    eofs_desc[source] = eofs_desc[source] | pow(2, id_operator)

                # The subtree j is the right argument of the plan.
                for source in subtree_j.sources.keys():
                    if len(set(sources[source]) & join_variables) > 0:
                        # TODO: Change the next q for len of something
                        operators_desc[id_operator].update({source: 1})
                    sources_desc[source] = sources_desc[source] | pow(2, id_operator)

                    # TODO: check this.
                    eofs_operators_desc[id_operator].update({source: 1})
                    eofs_desc[source] = eofs_desc[source] | pow(2, id_operator)

                plan_order[id_operator] = tree_height
                operators_vars[id_operator] = join_variables
                tree_height = tree_height + 1

                # Place physical operator estimating cardinality.


                if isinstance(star_tree, IndependentOperator):
                    #res = star_tree.total_res + subtree_j.total_res
                    # res = ((star_tree.total_res+ subtree_j.total_res)/2.0) * (1 + (min(star_tree.total_res, subtree_j.total_res) / float(max(star_tree.total_res, subtree_j.total_res))))
                    #res = (2.0 * star_tree.total_res * subtree_j.total_res) / (star_tree.total_res+ subtree_j.total_res)
                    res = estimate_card(star_tree.total_res, subtree_j.total_res)
                    # Place a Nested Loop join.
                    if star_tree.total_res < (subtree_j.total_res / 100.0):
                        subtree_j = DependentOperator(subtree_j.sources, subtree_j.server, subtree_j.query,
                                                      subtree_j.sources_desc, subtree_j.vars, subtree_j.total_res)
                        op = Xnjoin(id_operator, join_variables, eddies)
                        operators.append(op)
                        star_tree = TreePlan(op, all_variables, join_variables, sources,
                                             star_tree, subtree_j, tree_height, 0)
                        operators_sym.update({id_operator: False})

                    # Place a Symmetric Hash join.
                    else:
                        op = Fjoin(id_operator, join_variables, eddies)
                        operators.append(op)
                        star_tree = TreePlan(op, all_variables, join_variables, sources,
                                             star_tree, subtree_j, tree_height, res)
                        independent_sources = independent_sources + 1
                        operators_sym.update({id_operator: True})
                else:
                    # TODO: new change here
                    res = estimate_card(star_tree.total_res, subtree_j.total_res)
                    #res = (2.0 * star_tree.total_res * subtree_j.total_res) / (star_tree.total_res + subtree_j.total_res)
                    #res = (star_tree.total_res + subtree_j.total_res) / 2
                    if (star_tree.total_res / float(subtree_j.total_res) < 0.30) or (subtree_j.total_res > 100*1000 and star_tree.total_res < 100*1000) or (subtree_j.total_res < 100*5):
                        subtree_j = DependentOperator(subtree_j.sources, subtree_j.server, subtree_j.query,
                                                      subtree_j.sources_desc, subtree_j.vars, subtree_j.total_res)
                        op = Xnjoin(id_operator, join_variables, eddies)
                        operators.append(op)
                        star_tree = TreePlan(op, all_variables, join_variables, sources,
                                             star_tree, subtree_j, tree_height)
                        operators_sym.update({id_operator: False})
                    else:
                        op = Fjoin(id_operator, join_variables, eddies)
                        operators.append(op)
                        star_tree = TreePlan(op, all_variables,
                                             join_variables, sources, star_tree, subtree_j, tree_height, res)
                        independent_sources = independent_sources + 1
                        operators_sym.update({id_operator: True})
                id_operator += 1

        # Add current tree to the list of stars and
        # remove from the list of subtrees to process.
        stars.append(star_tree)
        for elem in to_delete:
            subtrees.remove(elem)

    # Stage 2: Build bushy tree to combine SSGs with common variables.
    while len(stars) > 1:

        subtree_i = stars.pop(0)

        for j in range(0, len(stars)):
            subtree_j = stars[j]

            all_variables = set(subtree_i.vars) | set(subtree_j.vars)
            join_variables = set(subtree_i.join_vars) & set(subtree_j.join_vars)

            # Case: There is a join between stars.
            if len(join_variables) > 0:

                # Update signatures.
                sources = {}
                sources.update(subtree_i.sources)
                sources.update(subtree_j.sources)

                operators_desc[id_operator] = {}
                operators_vars[id_operator] = join_variables
                eofs_operators_desc[id_operator] = {}

                for source in subtree_i.sources.keys():
                    # This models the restriction: a tuple must have the join
                    # variable instantiated to be routed to a certain join.
                    if len(set(sources[source]) & join_variables) > 0:
                        # TODO: Change the next 0 for len of something
                        operators_desc[id_operator].update({source: 0})
                    sources_desc[source] = sources_desc[source] | pow(2, id_operator)

                    # TODO: Check this.
                    eofs_operators_desc[id_operator].update({source: 0})
                    eofs_desc[source] = eofs_desc[source] | pow(2, id_operator)

                for source in subtree_j.sources.keys():
                    # This models the restriction: a tuple must have the join
                    # variable instantiated to be routed to a certain join.
                    if len(set(sources[source]) & join_variables) > 0:
                        # TODO: Change the next 1 for len of something
                        operators_desc[id_operator].update({source: 1})
                    sources_desc[source] = sources_desc[source] | pow(2, id_operator)

                    # TODO: Check this.
                    eofs_operators_desc[id_operator].update({source: 1})
                    eofs_desc[source] = eofs_desc[source] | pow(2, id_operator)

                plan_order[id_operator] = max(subtree_i.height, subtree_j.height)
                stars.pop(j)

                # Place physical operators between stars.
                if isinstance(subtree_j, IndependentOperator):
                    res = estimate_card(star_tree.total_res , subtree_j.total_res)
                    #res = (2.0 * star_tree.total_res * subtree_j.total_res) / (star_tree.total_res + subtree_j.total_res)
                    # res = ((star_tree.total_res+ subtree_j.total_res)/2.0) * (1 + (min(star_tree.total_res, subtree_j.total_res) / float(max(star_tree.total_res,subtree_j.total_res))))
                    #res = subtree_i.total_res + subtree_j.total_res
                    # res = (subtree_i.total_res + subtree_j.total_res) / 2
                    # This case models a satellite, therefore apply cardinality estimation.
                    if subtree_i.total_res < (subtree_j.total_res/100.0):
                        # res = subtree_i.total_res + subtree_j.total_res

                        subtree_j = DependentOperator(subtree_j.sources, subtree_j.server, subtree_j.query,
                                                      subtree_j.sources_desc, subtree_j.vars, subtree_j.total_res)
                        op = Xnjoin(id_operator, join_variables, eddies)
                        operators.append(op)
                        stars.append(TreePlan(op, all_variables,
                                              join_variables, sources, subtree_i, subtree_j,
                                              max(subtree_i.height, subtree_j.height, res)))
                        # Adjust number of asynchronous leaves.
                        independent_sources = independent_sources - 1
                        operators_sym.update({id_operator: False})
                    else:
                        # res = subtree_i.total_res + subtree_j.total_res
                        op = Fjoin(id_operator, join_variables, eddies)
                        operators.append(op)
                        stars.append(TreePlan(op, all_variables, join_variables,
                                              sources, subtree_i, subtree_j,
                                              max(subtree_i.height, subtree_j.height, res)))
                        operators_sym.update({id_operator: True})
                else:
                    res = (subtree_i.total_res + subtree_j.total_res) / 2
                    # res = subtree_i.total_res + subtree_j.total_res
                    op = Fjoin(id_operator, join_variables, eddies)
                    operators.append(op)
                    stars.append(TreePlan(op, all_variables, join_variables,
                                          sources, subtree_i, subtree_j,
                                          max(subtree_i.height, subtree_j.height, res)))
                    operators_sym.update({id_operator: True})
                id_operator += 1
                break

        if len(subtrees) % 2 == 0:
            tree_height += 1

    tree_height += 1
    tree = stars.pop()

    # TODO: missing stage 3.

    # Adds the projection operator to the plan.
    if query.projection:
        op = Xproject(id_operator, query.projection, eddies)
        operators.append(op)
        tree = TreePlan(op,
                        tree.vars, tree.join_vars, tree.sources, tree, None, tree_height+1, tree.total_res)

        # Update signature of tuples.
        operators_sym.update({id_operator: False})
        operators_desc[id_operator] = {}
        eofs_operators_desc[id_operator] = {}
        for source in tree.sources:
            operators_desc[id_operator].update({source: 0})
            eofs_operators_desc[id_operator].update({source: 0})
            eofs_desc[source] = eofs_desc[source] | pow(2, id_operator)
            sources_desc[source] = sources_desc[source] | pow(2, id_operator)
        plan_order[id_operator] = tree_height
        operators_vars[id_operator] = tree.vars
        id_operator += 1
        tree_height += 1

    # Adds the distinct operator to the plan.
    if query.distinct:
        op = Xdistinct(id_operator, eddies)
        operators.append(op)
        tree = TreePlan(op, tree.vars, tree.join_vars, tree.sources,
                        tree, None, tree_height + 1, tree.total_res)

        # Update signature of tuples.
        operators_sym.update ({id_operator: False})
        operators_desc[id_operator] = {}
        eofs_operators_desc[id_operator] = {}
        for source in tree.sources:
            operators_desc[id_operator].update({source: 0})
            eofs_operators_desc[id_operator].update({source: 0})
            eofs_desc[source] = eofs_desc[source] | pow(2, id_operator)
            sources_desc[source] = sources_desc[source] | pow(2, id_operator)
        plan_order[id_operator] = tree_height
        operators_vars[id_operator] = tree.vars
        id_operator += 1
        tree_height += 1

    #print "operators_desc", operators_desc
    #print "sources_desc", sources_desc
    #print "eofs_operators_desc", eofs_operators_desc
    #return tree, tree.height, operators_desc, sources_desc, plan_order, operators_vars, independent_sources, eofs_operators_desc, operators_sym
    #print
    #print "operators_sym", operators_sym
    return tree, tree.height, operators_desc, sources_desc, plan_order, operators_vars, independent_sources, operators_desc, operators_sym, operators