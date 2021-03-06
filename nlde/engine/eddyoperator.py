"""
Created on Nov 23, 2011

@author: Maribel Acosta
"""
from Queue import Empty


class EddyOperator(object):

    def __init__(self, id, policy, pool, operators_desc, operators_input_queues,  operators_vars,
                 outputqueue, independent_sources=0, eofs_operators_desc={}, operators_sym={}, operators=[]):
        self.id = id
        self.policy = policy
        self.routing_buffer = pool[id]
        self.operators_desc = operators_desc
        self.operators_input_queues = operators_input_queues
        #self.operators_right_queues = operators_right_queues
        self.operators_vars = operators_vars
        self.outputqueue = outputqueue
        self.eofs = independent_sources
        self.eofs_operators_desc = eofs_operators_desc
        self.operators_not_sym = []
        self.incoming_operators = []
        self.eddies = pool
        self.finalize = False
        self.eof = None
        self.operators = operators
        self.end = False
        self.wait = True

        # Build list of non-symmetric operators.
        for o in operators_sym.keys():
            if not(operators_sym[o]):
                self.operators_not_sym.append(o)

        # Build list of operators that will send tuples to this eddy.
        for o in operators:
            if o.eddy == self.id:
                self.incoming_operators.append(o)

    def execute(self):

        while not self.end:

            try:
                # Get tuple from input queue (pool).
                tup = self.routing_buffer.get(self.wait)

                # Get the operators that have not been executed yet.
                operators = tup.get_operators()

                # Case: Tuple has been processed by all operators.
                if len(operators) == 0:

                    # Case: Found EOF tuple that has been processed by all operators.
                    if tup.data == "EOF":

                        if not self.eof:
                            self.eof = tup
                            self.eof.from_operator = self.id
                            self.wait = False
                            self.finalize = True

                            # Notify other eddies in the network to finalize.
                            for i in range(0, len(self.eddies)):
                                if i != self.id:
                                    self.eddies[i].put(tup)

                    # Case: Produce query solution.
                    else:
                        self.outputqueue.put(tup)

                # Case: Route tuple to pending physical operators.
                else:

                    if operators[0] in self.operators_not_sym:
                        # First execute mandatory non-symmetric operators.
                        operator = operators[0]
                    else:
                        # Select the next operators to execute, according to the routing policy.
                        operator = self.policy.select_operator(operators, self.operators_desc, tup, self.operators_vars, self.operators_not_sym)

                    # Update tuple the destination of the tuple.
                    tup.to_operator = operator

                    # Add the tuple to the queue of the selected operators.
                    sources = list(set(tup.sources) & set(self.operators_desc[operator].keys()))
                    desc = self.operators_desc

                    queue = desc[operator][sources[0]]
                    self.operators_input_queues[operator][queue].put(tup)

                    #if desc[operator][sources[0]] == -1:
                    #    self.operators_left_queues[operator].put(tup)
                    #    queue = -1
                    #else:
                    #    self.operators_right_queues[operator].put(tup)
                    #    queue = 1

                    # Update priorities according to the routing policy.
                    self.policy.update_priorities(tup, queue)

            except Empty:

                # Last phase of execution.
                if self.finalize:
                    # Check if there exists a physical operator that is still working.
                    op_active = 0
                    in_empty = True
                    for op in self.operators:
                        i = op.id_operator
                        for q in self.operators_input_queues[i]:
                            in_empty = in_empty & q.empty()
                        #in_empty = in_empty & self.operators_left_queues[i].empty() & self.operators_right_queues[i].empty()

                    for op in self.operators:
                        op_active = op_active | op.probing.value

                    # If all the the operators are finished and there are no tuples in the routing buffer,
                    # then finish.
                    if not(op_active) and in_empty and self.routing_buffer.empty() and not(self.end):
                        self.end = True
                        self.outputqueue.put(self.eof)