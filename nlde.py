#!/usr/bin/env python

"""
Created on Mar 25, 2015

@author: Maribel Acosta
"""
import argparse
import os
import signal
import sys
from multiprocessing import active_children, Queue
from time import time

from nlde.engine.eddynetwork import EddyNetwork
from nlde.policy.nopolicy import NoPolicy
from nlde.policy.ticketpolicy import TicketPolicy
from nlde.policy.uniformrandompolicy import UniformRandomPolicy


def get_options():

    parser = argparse.ArgumentParser(description="nLDE: An engine to execute "
                                                 "SPARQL queries over Triple Pattern Fragments")

    # nLDE arguments.
    parser.add_argument("-s", "--server",
                        help="URL of the triple pattern fragment server (required)")
    parser.add_argument("-f", "--file",
                        help="file name of the SPARQL query (required, or -q)")
    parser.add_argument("-q", "--query",
                        help="SPARQL query (required, or -f)")
    parser.add_argument("-r", "--results",
                        help="format of the output results",
                        choices=["y", "n", "all"],
                        default="y")
    parser.add_argument("-e", "--eddies",
                        help="number of eddy processes to create",
                        type=int,
                        default=2)
    parser.add_argument("-p", "--policy",
                        help="routing policy used by eddy operators",
                        choices=["NoPolicy", "Ticket", "Random"],
                        default="NoPolicy")
    parser.add_argument("-t", "--timeout",
                        help="query execution timeout",
                        type=int)
    args = parser.parse_args()

    # Handling mandatory arguments.
    err = False
    msg = []
    if not args.server:
        err = True
        msg.append("error: no server specified. Use argument -s to specify the address of a server.")

    if not args.file and not args.query:
        err = True
        msg.append("error: no query specified. Use argument -f or -q to specify a query.")

    if err:
        parser.print_usage()
        print "\n".join(msg)
        sys.exit(1)

    return args.server, args.file, args.query, args.eddies, args.timeout, args.results, args.policy, args.explain


class NLDE(object):
    def __init__(self, source, queryfile, query, eddies, timeout, printres, policy_str, explain):
        self.source = source
        self.queryfile = queryfile
        self.query = query
        self.query_id = ""
        self.eddies = eddies
        self.timeout = timeout
        self.printres = printres
        self.policy_str = policy_str
        self.explain = explain
        self.network = None
        self.p_list = None
        self.res = Queue()

        # Open query.
        if self.queryfile:
            self.query = open(self.queryfile).read()
            self.query_id = self.queryfile[self.queryfile.rfind("/") + 1:]

        # Set routing policy.
        if self.policy_str == "NoPolicy":
            self.policy = NoPolicy()
        elif self.policy_str == "Ticket":
            self.policy = TicketPolicy()
        elif self.policy_str == "Random":
            self.policy = UniformRandomPolicy()

        # Set execution variables.
        self.init_time = None
        self.time_first = None
        self.time_total = None
        self.card = 0
        self.xerror = ""

        # Set execution timeout.
        if self.timeout:
            signal.signal(signal.SIGALRM, self.call_timeout)
            signal.alarm(self.timeout)

    def execute(self):
        # Initialization timestamp.
        self.init_time = time()

        # Create eddy network.
        network = EddyNetwork(self.query, self.policy, source=self.source, n_eddy=self.eddies, explain=self.explain)
        self.p_list = network.p_list

        if self.printres == "y":
            self.print_solutions(network)
        elif self.printres == "all":
            self.print_all(network)
        else:
            self.print_basics(network)

        self.summary()

    # Print only basic stats, but still iterate over results.
    def print_basics(self, network):

        network.execute(self.res)

        # Handle the first query answer.
        ri = self.res.get(True)
        self.time_first = time() - self.init_time
        count = 0
        if ri.data == "EOF":
            count = count + 1
        else:
            self.card = self.card + 1

        # Handle the rest of the query answer.
        while count < network.n_eddy:
            ri = self.res.get(True)
            if ri.data == "EOF":
                count = count + 1
            else:
                self.card = self.card + 1

        self.time_total = time() - self.init_time

    # Print only solution mappings.
    def print_solutions(self, network):

        network.execute(self.res)

        # Handle the first query answer.
        ri = self.res.get(True)
        self.time_first = time() - self.init_time
        count = 0
        if ri.data == "EOF":
            count = count + 1
        else:
            self.card = self.card + 1
            print str(ri.data)

        # Handle the rest of the query answer.
        while count < network.n_eddy:
            ri = self.res.get(True)
            if ri.data == "EOF":
                count = count + 1
            else:
                self.card = self.card + 1
                print str(ri.data)

        self.time_total = time() - self.init_time

    # Print all stats for each solution mapping.
    def print_all(self, network):

        network.execute(self.res)

        # Handle the first query answer.
        ri = self.res.get(True)
        self.time_first = time() - self.init_time
        count = 0
        if ri.data == "EOF":
            count = count + 1
        else:
            self.card = self.card + 1
            print self.query_id + "\t" + str(ri.data) + "\t" + str(self.time_first) + "\t" + str(self.card)

        # Handle the rest of the query answer.
        while count < network.n_eddy:
            ri = self.res.get (True)
            if ri.data == "EOF":
                count = count + 1
            else:
                self.card = self.card + 1
                t = time() - self.init_time
                print self.query_id + "\t" + str(ri.data) + "\t" + str(t) + "\t" + str(self.card)

        self.time_total = time() - self.init_time

    # Final stats of execution.
    def summary(self):
        print self.query_id + "\t" + str(self.time_first) + "\t" + str(self.time_total) + \
              "\t" + str(self.card) + "\t" + str(self.xerror)

    # Timeout was fired.
    def call_timeout(self, sig, err):
        self.time_total = time() - self.init_time
        self.finalize()
        self.summary()
        sys.exit(1)

    # Finalize execution: kill sub-processes.
    def finalize(self):

        self.res.close()

        while not self.p_list.empty():
            pid = self.p_list.get()
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError as e:
                pass

        for p in active_children():
            try:
                p.terminate()
            except OSError as e:
                pass


if __name__ == '__main__':
    (fragment, queryfile, query, eddies, timeout, printres, policy_str, explain) = get_options()

    nlde = NLDE(fragment, queryfile, query, eddies, timeout, printres, policy_str, explain)
    nlde.execute()
    nlde.finalize()
    sys.exit(0)
