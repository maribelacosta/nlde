"""
Created on Mar 23, 2015

@author: Maribel Acosta
"""
from eddyoperator import EddyOperator
from nlde.util.sparqlparser import parse
from nlde.util.explainplan import explain_plan
from nlde.planner.optimizer import IndependentOperator, create_plan
from multiprocessing import Process, Queue


class EddyNetwork(object):
    
    def __init__(self, query, policy, source="", n_eddy=2, explain=False):
        self.query = query
        self.policy = policy
        self.source = source
        self.n_eddy = n_eddy
        self.explain = explain
        self.eofs = 0

        self.independent_operators = []
        self.join_operators = []
        self.eddy_operators = []
        
        self.eddies_queues = []
        self.operators_input_queues = {}
        self.operators_left_queues = []
        self.operators_right_queues = []

        self.p_list = Queue()

        self.tree = None
        self.operators_desc = None
        self.sources_desc = None
        self.eofs_operators_desc = None

    def execute(self, outputqueue):
          
        # Parse SPARQL query.
        queryparsed = parse(self.query)

        # Create plan.
        (self.tree, tree_height, self.operators_desc, self.sources_desc,
         plan_order, operators_vars, independent_sources, self.eofs_operators_desc,
         operators_sym, operators) = create_plan(queryparsed, self.n_eddy, self.source)

        if self.explain:
            explain_plan(self.tree)

        #print "Plan"
        #print str(self.tree)

        self.eofs = independent_sources
        self.policy.initialize_priorities(plan_order)
        
        # Create eddies queues.                
        for i in range(0, self.n_eddy+1):
            self.eddies_queues.append(Queue())

        # Create operators queues (left and right).
        for op in operators:
            self.operators_input_queues.update({op.id_operator: []})
            for i in range(0, op.independent_inputs):
                self.operators_input_queues[op.id_operator].append(Queue())
        #for i in range(0, len(self.operators_desc)):
        #for i in self.operators_desc.keys():
        #    self.operators_input_queues.update({i: []})
        #    for j in self.operators_desc[i].keys():

                #self.operators_input_queues[i].update(j:{})#append(Queue())
            #self.operators_right_queues.append(Queue())

        # Create eddy operators and execute them.
        for i in range(1, self.n_eddy+1):
            eddy = EddyOperator(i, self.policy, self.eddies_queues, self.operators_desc, self.operators_input_queues,
                                operators_vars, outputqueue, independent_sources, self.eofs_operators_desc,
                                operators_sym, operators)
            p = Process(target=eddy.execute)
            p.start()
            self.p_list.put(p.pid)

        self.tree.execute(self.operators_input_queues, self.eddies_queues, self.p_list, self.operators_desc)
