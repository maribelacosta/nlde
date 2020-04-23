'''
Created on Nov 27, 2011

@author: macosta
'''
from nlde.operators.operatorstructures import Tuple
from eddyoperator import EddyOperator
from nlde.util.sparqlparser import SPARQLParser
from nlde.operators.xnjoin import Xnjoin
from nlde.operators.fjoin import Fjoin
from nlde.util.ldfparser import LDFParser
from multiprocessing import Process, Queue
import socket
import urllib
import string
import httplib 
from random import randint


class EddyNetwork(object):
    
    def __init__(self, query, policy, buffersize=0, source="", n_eddy=2): # , n_eddy, n_operator, operators_desc, policy
        self.query = query
        self.policy = policy
        self.source = source
        self.n_eddy = n_eddy
        self.eofs = 0
        
        self.independent_operators = []
        self.join_operators = []
        self.eddy_operators = []
        
        self.eddies_queues = []
        self.operators_left_queues = []
        self.operators_right_queues = []
        

    
    def execute(self, outputqueue):
          
        # Parse query from SPARQL 1.1.
        queryparsed = SPARQLParser(self.query, self.source)
        project_vars = queryparsed.getAttributes()
        subqueries = queryparsed.getSubQueries(project_vars)
        
        independent_operators = []
        id_operator = 0
        sources_desc = {}
        eofs_desc = {}
        
        # Create initial plan (bushy tree).  
        for subquery in subqueries:
            sources_desc.update({id_operator:0}) 
            eofs_desc.update({id_operator:0})
            independent_operators.append(IndependentOperator(id_operator, subquery[0], subquery[1], sources_desc, subquery[2], self.n_eddy, eofs_desc))
            id_operator += 1

        (self.tree, tree_height, self.operators_desc, self.sources_desc, plan_order, operators_vars, independent_sources, self.eofs_operators_desc, operators_sym) = createBushyTree(independent_operators, sources_desc, self.n_eddy, eofs_desc)
        
        
        # TODO: as parameter
        #self.n_eddy = tree_height + 1
        print "independent_sources", independent_sources
        self.eofs = independent_sources
        
        self.policy.initialize_priorities(plan_order)
        
        # Create eddies queues.                
        for i in range(0, self.n_eddy+1):
            #print "HERE ", i, self.n_eddy
            self.eddies_queues.append(Queue())
        
        #print "HERE after queues for eddies"        
        # Create operators queues (left and right).
        for i in range(0, len(self.operators_desc)):
            self.operators_left_queues.append(Queue())
            self.operators_right_queues.append(Queue())
            
        #print "HERE after queues for operators"
        
        # Create eddy sink operators.
        #eddy_sink = EddySink(0, self.policy, self.eddies_queues[0], len(self.sources_desc))
        #p = Process(target=eddy_sink.execute, args=(outputqueue,))
        #p.start() 
                                
        # Create eddy operators and execute them.
        for i in range(1, self.n_eddy+1):
            eddy = EddyOperator(i, self.policy, 
                        self.eddies_queues[i], self.operators_desc, 
                        self.operators_left_queues, self.operators_right_queues, operators_vars, outputqueue, independent_sources, self.eofs_operators_desc, operators_sym)
            p = Process(target=eddy.execute)
            p.start()
            

        #print "Tree height:", self.n_eddy - 1
        #print "Operators Desc:", self.operators_desc   
        #print "Sources Desc:", self.sources_desc  
        #print "Priority Table:", self.policy.priority_table
        #print "Before executing plan .."    
        # Execute "tree plan": Create joins and contact sources.
        self.tree.execute(self.operators_left_queues, self.operators_right_queues, self.eddies_queues)    
        

class DependentOperator(object):
    
      
    def __init__(self, sources, server, query, sources_desc, vars=[], res=0):
        
        self.server = server
        #self.filename = filename
        #self.q = None
        self.q = Queue()
        self.sources = sources#{sources : vars}
        self.server = server
        self.query = query
        self.sources_desc = sources_desc
        self.vars = vars
        self.join_vars = vars
        self.total_res = res
        self.height = 0
        
        
    def execute(self, variables, instances, outputqueue):
        
        #print "QUERY IN DEPENDENT OPERATOR", self.query, instances
        query = self.query
        query = " ".join(query)
        variables = list(variables)
        inst = {}
        # Replace in the query, the instance that is derreferenced.
        
        #print "variables in DO", variables
        for i in variables:
            inst.update({i: instances[i]})
            inst_aux = str(instances[i]).replace(" ", "%%%")
            #query = string.replace(query, "?" + variables[i], "", 1)
            query = string.replace(query, "?"+i,  inst_aux)
            
        #to_get = []
        
        query = query.split(" ")  
        for i in range(0, len(query)):
            query[i] = query[i].replace("%%%", " ")
            
        #print "QUERY BEFORE CONTACT SERVER IN DO", query, instances 
        aux_queue = Queue() 
        #contactLDFServer(self.server, query, aux_queue, self.vars)
        self.p = Process(target=contactLDFServer,
                         args=(self.server, query, aux_queue, self.vars,))
        self.p.start()
        sources = self.sources.keys()
        
        while True:
            data  = aux_queue.get(True)
            #print self.sources 
            #if (self.sources.keys()[0] == 0):
                #print "reading data from contactsource", data, instances, self.sources, query
            ready = self.sources_desc[self.sources.keys()[0]] 
            done  = 0
            #print "data", data
            if (data == "EOF"):
                break
            data.update(inst)
            #data.update({variables[0] : instances[variables[0]]})
            #print "data", data
            ready = self.sources_desc[self.sources.keys()[0]] 
            #done  = 0
            # TODO: changed this
            #sources = self.sources
            #    sources = self.sources.keys()
            
            
            #eddy = bitCount(ready)
            #print "HERE in dependent operators", data, instances
            outputqueue.put(Tuple(data, ready, done, sources))
            
            
        #print "Finishing in DependentOperator"
        #Close the queue
        aux_queue.close()     
        outputqueue.put(Tuple("EOF", ready, done, sources))
        self.p.join()
        
    

class IndependentOperator(object):
    '''
    Implements an operators that can be resolved independently.
    
    It receives as input the url of the server to be contacted,
    the filename that contains the query, the header size of the 
    response message and the buffer size (length of the string)
    of the messages.
    
    The execute() method reads tuples from the input queue and
    place them in the output queue.
    '''
    def __init__(self, sources, server, query, sources_desc, vars=[], eddies=2, eofs_desc={}):
        self.server = server
        #self.filename = filename
        #self.q = None
        self.q = Queue()
        self.sources = {sources : vars}
        self.server = server
        self.query = query
        self.sources_desc = sources_desc
        self.vars = vars
        self.join_vars = vars
        self.total_res = -1
        self.height = 0
        self.eddies = eddies 
        self.eofs_desc = eofs_desc
       
        #print "Independent Operator init:", self.sources, self.sources_desc
        #print "Independent Operator init eof:", self.sources, self.eofs_desc
        #self.p.join()

    def execute(self, left, right, outputqueues):
        # Evaluate the independent operators.
        #print "Independent Operator execute:", self.sources, self.sources_desc
        #print "Independent Operator execute eof:", self.sources, self.eofs_desc
        print "left", type(left), "right", type(right), left, right, self.sources_desc, self.sources
        if (left == None):
            outq = right
        else:
            outq = left
        #contactLDFServer(self.server, self.query, self.q, self.vars)
        self.p = Process(target=contactLDFServer,
                         args=(self.server, self.query, self.q, self.vars,))
        #self.p = Process(target=contactSimulator,
        #                 args=(self.server, self.query, 1500, self.q,))
        
        self.p.start()
        
        ready = self.sources_desc[self.sources.keys()[0]] 
        done  = 0
        sources = self.sources.keys()
        
        while True:
            data  = self.q.get(True)
            
            if (data == "EOF"):
                break
            #print "data in IO", data
            #TODO: change this
            #sources = self.sources
            
            eddy = randint(1, self.eddies) 
            #print "eddy in IO", self.eddies, eddy, len(outputqueues), outputqueues
            #eddy = bitCount(ready)
            #outputqueues[eddy].put(Tuple(data, ready, done, sources))
            outq.put(Tuple(data, ready, done, sources))
            
            
        
        eddy = randint(1, self.eddies)
        ready_eof = self.eofs_desc[self.sources.keys()[0]] 
        #print "HERE EOF IN IO", sources, ready, done, self.sources_desc
        #outputqueues[eddy].put(Tuple("EOF", ready_eof, done, sources))
        
        outq.put(Tuple("EOF", ready_eof, done, sources))    
        self.p.join()


class TreePlan(object):
    '''
    Represents a plan to be executed by the engine.
    
    It is composed by a left node, a right node, and an operators node.
    The left and right nodes can be leaves to contact sources, or subtrees.
    The operators node is a physical operators, provided by the engine.
    
    The execute() method evaluates the plan.
    It creates a process for every node of the plan.
    The left node is always evaluated.
    If the right node is an independent operators or a subtree, it is evaluated.
    '''
    def __init__(self, operator, vars, join_vars, sources, left, right, height=0, res=0):
        self.operator = operator
        self.vars = vars
        self.join_vars = vars
        self.sources = sources
        self.left = left
        self.right = right
        self.height = height
        self.total_res = res


    def execute(self, operators_left_queues, operators_right_queues, eddies_queues):
        # Evaluates the execution plan. 
         
        if self.left:
            if (self.left.__class__.__name__ == "IndependentOperator"):
                # Data from sources goes directly to operators.
                p1 = Process(target=self.left.execute, args=(operators_left_queues[self.operator.id_operator], None, eddies_queues,))
            else:
                p1 = Process(target=self.left.execute, args=(operators_left_queues, operators_right_queues, eddies_queues,))
            #print "left", p1#, p1.pid()
            p1.start()
          
        if (self.right.__class__.__name__ == "TreePlan") or (self.right.__class__.__name__ == "IndependentOperator"):
            if (self.right.__class__.__name__ == "IndependentOperator"):
                # Data from sources goes directly to operators.
                p2 = Process(target=self.right.execute, args=(None, operators_right_queues[self.operator.id_operator], eddies_queues,))
            else:
                p2 = Process(target=self.right.execute, args=(operators_left_queues, operators_right_queues, eddies_queues,))      
            #print "right", p2.pid()
            p2.start()
            right = operators_right_queues[self.operator.id_operator]
        #elif (self.right.__class__.__name__ == "IndependentOperator"):
        #    self.right.execute(operators_left_queues, operators_right_queues, eddies_queues)
        #    right = operators_right_queues[self.operators.id_operator]
        else:
            right = self.right
        
                                                            
        # Create a process for the operators node.
        #print "operators id in execute tree", self.operators.id_operator
        self.p = Process(target=self.operator.execute, args=(operators_left_queues[self.operator.id_operator], right, eddies_queues,))
        #print "operators in tree", self.p
        self.p.start()

def getMetadataLDF(server, query):
    
    #print "query", query, server
    # Extract server information.
    referer = server
    server = server.split("http://")[1]
    (server, path) = server.split("/", 1)
    host_port = server.split(":")
    port = 80 if len(host_port) == 1 else host_port[1]
    
    # Prepare parameters and header of request
    params = urllib.urlencode({'subject': query[0], 'predicate':  query[1], "object": query[2]})
    headers = {"User-Agent": "2.7", "Accept": "*/*", "Referer": referer, "Host": server, "Accept":"application/json"}
    
    
    # Establish connection and get response from server.
    print "server", server, port
    conn = httplib.HTTPConnection(server)
    #conn.set_debuglevel(1)
    conn.request("GET", "/" + path + "?" + params, None, headers)
    #print conn
    response = conn.getresponse()  
    total = -1
    
    if (response.status == httplib.OK):
        res = response.read()
        #print "res",res
        pos1 = res.find('"void:triples": ')
        pos2 = res[pos1:].find(",")
        total = int(res[pos1+len('"void:triples": '):pos1+pos2]) 
    
    print "total metadata", total, query 
    return total
    
            

def contactLDFServer(server, query, queue, vars):
    #print "server", server
    #print "query", query
    
    # Extract server information.
    referer = server
    server = server.split("http://")[1]
    (server, path) = server.split("/", 1)
    host_port = server.split(":")
    port = 80 if len(host_port) == 1 else host_port[1] 
    
    template = 0
    vars = []
    # Build query for request.
    #print query[0]
    subject = query[0]#).encode("utf-8")
    if (subject[0] == "?" or subject[0] == "$"):
        template = template | 4
        vars.append(subject[1:])
        
    #    subject = ""
    predicate = query[1]
    if (predicate[0] == "?" or predicate[0] == "$"):
        template = template | 2
        vars.append(predicate[1:])
    #    predicate = "" 
    value = query[2]
    if (value[0] == "?" or value[0] == "$"):
        template = template | 1  
        vars.append(value[1:])
    params = urllib.urlencode({'subject': subject, 'predicate':  predicate, "object": value})
    #print params
    headers = {"User-Agent": "2.7", "Referer": referer, "Host": server, "Accept":"application/json;q=1.0"}
    
    #nextpage = True
    pagesize = 100
    count = -1
    total = 0
    page = 1
    
    #if True:
    while(count < total):
        # Establish connection and get response from server.
        conn = httplib.HTTPConnection(server)
        
        #conn.set_debuglevel(1)
        conn.request("GET", "/" + path + "?" + params, None, headers)
        #print "server", server, "path", path, "params", params
        response = conn.getresponse()  
        #print "query" ,query,total, query[0].decode("utf-8")
        #print "response", response, query, count, total
        #total = 0
    
        if (response.status == httplib.OK):
            res = response.read()
            
            
            #print "res", query, res
            
            pos1 = res.find('"void:triples": ')
            pos2 = res[pos1:].find(",")
            total = int(res[pos1+len('"void:triples": '):pos1+pos2])
            
            #print "total retreive answers", total, query
            #pos3 = res.find('"hydra:nextPage": {')
            #pos4 = res[pos3:].rfind('},')
            #nextpage =  res[pos3:pos3+pos4]
            #pos6 = res[pos3:pos3+pos4].rfind('"')
            #nextpage = res[pos3+len('"hydra:nextPage": {'):pos3+pos6].rstrip("\n").lstrip("\n")
            #pos5 = nextpage.rfind("?")
            #nextpage = nextpage[pos5+1:len(nextpage)]
            #params = nextpage
            page = page + 1
            params = urllib.urlencode({'subject': subject, 'predicate':  predicate, "object": value, "page": page})
            #print "params", params

            myres = eval(res)
            #time1 = time()
            LDFParser(template, myres["@graph"], vars, queue, total, count)
            #time2 = time()
            count = count + pagesize
            
            #print "time parsing", time2-time1
            #print "count", count, total, query 
        else:
            break
    
    #Close the queue     
    queue.put("EOF")
       
                       
#def contactEndpoint(server, query, queue):
#    '''
#    Contacts the datasource (i.e. endpoint).
#    Every tuple in the answer is represented as Python dictionaries
#    and is stored in a queue. 
#    '''
#    #print "Contacting source:", server
#    #print "Subquery:", query
#    
#    # Build the query and contact the source.
#    sparql = SPARQLWrapper(server)
#    sparql.setQuery(query)
#    sparql.setReturnFormat(JSON)
#    res = sparql.query().convert()
#    
#
#    for x in res['results']['bindings']:
#        for key, props in x.iteritems():
#            x[key] = props['value']
#
#    reslist = res['results']['bindings']
#    
#    # Every tuple is added to the queue.
#    for elem in reslist:
#        queue.put(elem)
#
#    #Close the queue     
#    queue.put("EOF")
    

def contactSimulator(server, query, buffersize, queue):
    '''
    Contacts the datasource (i.e. endpoint).
    Every tuple in the answer is represented as Python dictionaries
    and is stored in a queue. 
    '''
    # Encode the query as an url string.
    query = urllib.quote(query.encode('utf-8'))
    
    #Get host and port from "server".
    print server
    [http, server] = server.split("http://")    
    host_port = server.split(":")  
    
    # Create socket, connect it to server and send the query.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host_port[0], int(host_port[1])))
    s.send("GET ?query=" + query)
    s.shutdown(1) 
    
    
    # Get the size of the package from the header.
    aux = ""

    #Receive the rest of the messages.
    while True: 
        data = s.recv(buffersize)
        data = aux + data
        reslist = data.split('\n')
        for elem in reslist:
            if (len(elem)>0 and elem[0]=="{" and elem[len(elem)-1]=="}"):
                queue.put(eval(elem.rstrip()))
            else:
                aux = elem    
                            
        if (string.find(data, "EOF")>-1):
            #print "data", data
            break

    #Close the queue     
    queue.put("EOF")
    #Close the connection    
    s.close()


def createBushyTree(subtrees, sources_desc, eddies, eofs_desc):
    
    tree_height = 0
    id_operator = 0
    operators_desc = {}
    plan_order = {} 
    operators_vars = {}
    ordered_subtrees = [] 
    stars = []
    independent_sources = 0
    eofs_operators_desc = {}
    operators_sym = {}

    for leaf in subtrees:
        leaf.total_res = getMetadataLDF(leaf.server, leaf.query)
        ordered_subtrees.append(leaf.total_res)
    
    # Order leaves depending on the cardinality of fragments.  
    keydict = dict(zip(subtrees, ordered_subtrees))
    subtrees.sort(key=keydict.get)
    
    # Generate left-linear index nested stars. 
    while (len(subtrees) > 0): 
        to_delete = []
        star_tree = subtrees.pop(0)
        star_vars = star_tree.vars
        tree_height = 0
        independent_sources = independent_sources + 1 
        
        #print
        #print "root of star", star_tree.sources
        
        for j in range(0, len(subtrees)):
            subtree_j = subtrees[j] 
            join_variables = (set(star_vars) & set(subtree_j.join_vars)) 
            all_variables  = set(star_tree.vars) | set(subtree_j.vars)
             
            
            #print "processing", subtree_j.sources, type(subtree_j)
            #print "join_variables", join_variables
            
            if (len(join_variables) == 1 and addTriplePatternToCurrentStar(subtree_j, star_tree, subtrees, 100)):
            #if (len(join_variables) == 1):
            #if (join_variables != set([])):
                
                #if (addTriplePatternToCurrentStar(subtree_j, star_tree, subtrees, 100)):
                #    pass
                
                star_vars = set(star_vars) | set(subtree_j.join_vars)
                
                to_delete.append(subtree_j)
                
                sources = {}
                sources.update(star_tree.sources)
                sources.update(subtree_j.sources)
                operators_desc[id_operator] = {}
                operators_vars[id_operator] = join_variables
                eofs_operators_desc[id_operator] = {}
                       
                for source in star_tree.sources.keys():
                    if (set(sources[source]) & join_variables != set([])):
                        operators_desc[id_operator].update({source: -1})
                        sources_desc[source] = sources_desc[source] | pow(2, id_operator) 
                    
                    eofs_operators_desc[id_operator].update({source: -1})
                    eofs_desc[source] = eofs_desc[source] | pow(2, id_operator) 
                
                for source in subtree_j.sources.keys():
                    if (set(sources[source]) & join_variables != set([])):
                        operators_desc[id_operator].update({source: 1})               
                        sources_desc[source] = sources_desc[source] | pow(2, id_operator)
                    
                    eofs_operators_desc[id_operator].update({source: 1})
                    eofs_desc[source] = eofs_desc[source] | pow(2, id_operator) 
                        
              
                plan_order[id_operator] = tree_height
                operators_vars[id_operator] = join_variables
                
                tree_height = tree_height + 1
                
                res = star_tree.total_res + subtree_j.total_res
                if isinstance(star_tree, IndependentOperator):
                    #print
                    #print "first triple", star_tree, star_tree.total_res < (subtree_j.total_res /100.0)
                    #print
                    #if True:
                    if star_tree.total_res < (subtree_j.total_res / 100.0):
                        #print "-------------------------"
                        print "Xnjoin < 100", id_operator,  star_tree.sources, subtree_j.sources, "cost", star_tree.total_res
                        subtree_j = DependentOperator(subtree_j.sources, subtree_j.server, subtree_j.query, subtree_j.sources_desc, subtree_j.vars, star_tree.total_res)    
                        star_tree = TreePlan(Xnjoin(id_operator, join_variables, eddies), all_variables, join_variables, sources, star_tree, subtree_j, tree_height, res)
                        operators_sym.update({id_operator : False})
                    else:
                        #print "HERE!", star_tree.sources, subtree_j.sources
                        print "Xgjoin", id_operator, star_tree.sources, subtree_j.sources, "cost", res
                        star_tree = TreePlan(Fjoin(id_operator, join_variables, eddies), all_variables, join_variables, sources, star_tree, subtree_j, tree_height, res)
                        independent_sources = independent_sources + 1
                        operators_sym.update({id_operator : True})
                else:
                    if (star_tree.total_res <= subtree_j.total_res):
                        print "Xnjoin", id_operator, star_tree.sources, subtree_j.sources, "cost", star_tree.total_res
                        subtree_j = DependentOperator(subtree_j.sources, subtree_j.server, subtree_j.query, subtree_j.sources_desc, subtree_j.vars, star_tree.total_res)    
                        star_tree = TreePlan(Xnjoin(id_operator, join_variables, eddies), all_variables, join_variables, sources, star_tree, subtree_j, tree_height)
                        operators_sym.update({id_operator : False})
                    else:
                        print "Xgjoin", id_operator, star_tree.sources, subtree_j.sources, "cost", res 
                        star_tree = TreePlan(Fjoin(id_operator, join_variables, eddies), all_variables, join_variables, sources, star_tree, subtree_j, tree_height, res)
                        independent_sources = independent_sources + 1
                        operators_sym.update({id_operator : True})
                id_operator += 1
                #print "id_operator in loop", id_operator
                    
        stars.append(star_tree)
        for elem in to_delete:
            subtrees.remove(elem) 
        
    #print "independent_sources", independent_sources       
    print "stars", len(stars), stars
    #id_operator += 1
    #print "id_orpera", id_operator
    #for s in stars:
    
    # Stage 2: Add joins
    while len(stars) > 1:
        subtree_i = stars.pop(0)
        
        print "HeRE", len(stars)
        for j in range(0, len(stars)): 
            subtree_j = stars[j] 
            
            all_variables  = set(subtree_i.vars) | set(subtree_j.vars)
            join_variables = (set(subtree_i.join_vars) & set(subtree_j.join_vars)) # & all_variables
            
            print "join_vars", join_variables, subtree_i.join_vars, subtree_j.join_vars 
            print "joiny_vars", join_variables
            if (join_variables != set([])):
         
                sources = {}
                sources.update(subtree_i.sources)
                sources.update(subtree_j.sources)
                operators_desc[id_operator] = {}
                operators_vars[id_operator] = join_variables
                eofs_operators_desc[id_operator] = {}
                      
                for source in subtree_i.sources.keys():
                    
                    # This models the restriction: a tuple must have the join 
                    # variable instantiated to be routed to a certain join. 
                    if (set(sources[source]) & join_variables != set([])):
                        operators_desc[id_operator].update({source: -1})
                        sources_desc[source] = sources_desc[source] | pow(2, id_operator)
                    
                    eofs_operators_desc[id_operator].update({source: -1})
                    eofs_desc[source] = eofs_desc[source] | pow(2, id_operator) 
                
                for source in subtree_j.sources.keys():
                    # This models the restriction: a tuple must have the join 
                    # variable instantiated to be routed to a certain join.
                    if (set(sources[source]) & join_variables != set([])):
                        operators_desc[id_operator].update({source: 1})               
                        sources_desc[source] = sources_desc[source] | pow(2, id_operator)
                        
                    eofs_operators_desc[id_operator].update({source: 1})
                    eofs_desc[source] = eofs_desc[source] | pow(2, id_operator)
              
                plan_order[id_operator] = max(subtree_i.height,subtree_j.height)
                
                stars.pop(j)    
#              


#                # TODO: new, to detect the physical operators.
#                if isinstance(subtree_i, IndependentOperator) and isinstance(subtree_j, IndependentOperator):
#                    if subtree_i.total_res < subtree_j.total_res / 100.0: # TODO: fixed page size
#                        subtree_j = DependentOperator(subtree_j.sources, subtree_j.server, subtree_j.query, subtree_j.sources_desc, subtree_j.vars)    
#                        subtrees.append(TreePlan(Xnjoin(id_operator, join_variables), all_variables, join_variables, sources, subtree_i, subtree_j))
#                        print "Xnjoin",  subtree_i.sources, subtree_j.sources
#                    else:
#                        subtrees.append(TreePlan(Fjoin(id_operator, join_variables), all_variables, join_variables, sources, subtree_i, subtree_j))
#                        print "Xgjoin",  subtree_i.sources, subtree_j.sources
#               else:

                if isinstance(subtree_j, IndependentOperator):
                    print "SATELLITE!"
                    #print "first triple", star_tree, star_tree.total_res < (subtree_j.total_res /100.0)
                    #print
                    if subtree_i.total_res < (subtree_j.total_res / 100.0):
                        #print "-------------------------"
                        print "Xnjoin < 100", id_operator,  subtree_i.sources, subtree_j.sources, subtree_j.query
                        res = subtree_i.total_res + subtree_j.total_res
                        subtree_j = DependentOperator(subtree_j.sources, subtree_j.server, subtree_j.query, subtree_j.sources_desc, subtree_j.vars, star_tree.total_res)    
                        stars.append(TreePlan(Xnjoin(id_operator, join_variables, eddies), all_variables, join_variables, sources, subtree_i, subtree_j, max(subtree_i.height,subtree_j.height, res)))
                        independent_sources = independent_sources - 1
                        operators_sym.update({id_operator : False})
                    else:
                        #print "HERE!", star_tree.sources, subtree_j.sources
                        print "Xgjoin", id_operator, star_tree.sources, subtree_j.sources
                        res = subtree_i.total_res + subtree_j.total_res
                        stars.append(TreePlan(Fjoin(id_operator, join_variables, eddies), all_variables, join_variables, sources, subtree_i, subtree_j, max(subtree_i.height,subtree_j.height, res)))
                        #independent_sources = independent_sources + 1
                        operators_sym.update({id_operator : True})
                else:

                    res = subtree_i.total_res + subtree_j.total_res
                    stars.append(TreePlan(Fjoin(id_operator, join_variables, eddies), all_variables, join_variables, sources, subtree_i, subtree_j, max(subtree_i.height,subtree_j.height, res)))
                    print "Fjoin",  id_operator, subtree_i.sources, subtree_j.sources
                    operators_sym.update({id_operator : True})
#
#                #TODO: Check whether this forbids Cartesian product. 
#                #subtrees.append(TreePlan(Xgjoin(id_operator, join_variables), join_variables, sources, subtree_i, subtree_j))
#                #subtrees.append(TreePlan(SymmetricHashJoin(id_operator, join_variables), all_variables, sources, subtree_i, subtree_j))
                #print "Join", subtree_i.vars, "and", subtree_j.vars, "join for:", join_variables
                id_operator += 1
                break
#        
        if ((len(subtrees)%2) == 0):
            tree_height += 1
#        
    tree_height += 1  
    tree = stars.pop()
    return (tree, tree.height, operators_desc, sources_desc, plan_order, operators_vars, independent_sources, eofs_operators_desc, operators_sym)


def createBushyTreeOLD(subtrees, sources_desc):
    
    tree_height = 0
    id_operator = 0
    operators_desc = {}
    plan_order = {} 
    ordered_subtrees = [] 
    operators_vars = {}
    
    
    #print "here"
    
    for leaf in subtrees:
        #print "leaf", type(leaf)
        #total_res = 
        leaf.total_res = getMetadataLDF(leaf.server, leaf.query)
        ordered_subtrees.append(leaf.total_res)
    
    # Order leaves depeding on the selectivity 
    keydict = dict(zip(subtrees, ordered_subtrees))
    subtrees.sort(key=keydict.get)
    
    while (len(subtrees) > 1):
        #print "len(subtrees)", len(subtrees)
        subtree_i = subtrees.pop(0)
        
        for j in range(0, len(subtrees)): 
            subtree_j = subtrees[j]  
            
            #print "join_vars", set(subtree_i.join_vars), set(subtree_j.join_vars)
            
            all_variables  = set(subtree_i.vars) | set(subtree_j.vars)
            join_variables = (set(subtree_i.join_vars) & set(subtree_j.join_vars)) & all_variables
             
            if (join_variables != set([])):
                sources = {}
                sources.update(subtree_i.sources)
                sources.update(subtree_j.sources)
                operators_desc[id_operator] = {}
                operators_vars[id_operator] = join_variables
               
                for source in subtree_i.sources.keys():
                    if (set(sources[source]) & join_variables != set([])):
                        operators_desc[id_operator].update({source: -1})
                        sources_desc[source] = sources_desc[source] | pow(2, id_operator) 
                
                for source in subtree_j.sources.keys():
                    if (set(sources[source]) & join_variables != set([])):
                        operators_desc[id_operator].update({source: 1})               
                        sources_desc[source] = sources_desc[source] | pow(2, id_operator)
              
                plan_order[id_operator] = tree_height
                
                subtrees.pop(j)    
                
                # TODO: new, to detect the physical operators.
                if isinstance(subtree_i, IndependentOperator) and isinstance(subtree_j, IndependentOperator):
                    if subtree_i.total_res < (subtree_j.total_res / 100.0): # TODO: fixed page size
                        subtree_j = DependentOperator(subtree_j.sources, subtree_j.server, subtree_j.query, subtree_j.sources_desc, subtree_j.vars)    
                        subtrees.append(TreePlan(Xnjoin(id_operator, join_variables), all_variables, join_variables, sources, subtree_i, subtree_j))
                        print "Xnjoin",  subtree_i.sources, subtree_j.sources
                    else:
                        subtrees.append(TreePlan(Fjoin(id_operator, join_variables), all_variables, join_variables, sources, subtree_i, subtree_j))
                        print "Xgjoin",  subtree_i.sources, subtree_j.sources
                else:
                    subtrees.append(TreePlan(Fjoin(id_operator, join_variables), all_variables, join_variables, sources, subtree_i, subtree_j))
                    print "Xgjoin",  subtree_i.sources, subtree_j.sources
                
                #TODO: Check whether this forbids Cartesian product. 
                #subtrees.append(TreePlan(Fjoin(id_operator, join_variables), all_variables, join_variables, sources, subtree_i, subtree_j))
                #subtrees.append(TreePlan(Fjoin(id_operator, join_variables), join_variables, sources, subtree_i, subtree_j))
                #subtrees.append(TreePlan(SymmetricHashJoin(id_operator, join_variables), all_variables, sources, subtree_i, subtree_j))
                #print "Join", subtree_i.vars, "and", subtree_j.vars, "join for:", join_variables
                id_operator += 1
                break
        
        if ((len(subtrees)%2) == 0):
            tree_height += 1
        
    tree_height += 1  
    return (subtrees.pop(), tree_height, operators_desc, sources_desc, plan_order, operators_vars)


def addTriplePatternToCurrentStar(tp, current_plan, pending_subtrees, pagesize):
    
    # Compute the cost of adding the Triple Pattern (tp) to the plan built so far (current_plan) 
    cost_with_current_plan = 0
    if isinstance(current_plan, IndependentOperator):
    #if True:
        if current_plan.total_res < (tp.total_res / pagesize):
            # Add a njoin
            cost_with_current_plan = (current_plan.total_res)/100 + current_plan.total_res 
        else:
            # Add a gjoin
            cost_with_current_plan = (current_plan.total_res + tp.total_res) / 100
    else:
        if ((current_plan.total_res)/100) <= (tp.total_res/100):
            # Add an njoin
            cost_with_current_plan = (current_plan.total_res)/100 
        else:
            # Add a gjoin 
            cost_with_current_plan = (tp.total_res) / 100
            
    print 
    print "cost with current plan", tp.sources, current_plan.sources, cost_with_current_plan
    
    # Compute the cost of combining the triple pattern with another element from the pending subtrees to process.  
    # Note: pending_subtrees are ordered by cardinality (total_res), therefore it 
    # is always more convenient to perform tp NJOIN elem.
    cost_with_another_elem = None
    for elem in pending_subtrees:
        
        if elem == tp or (len(set(elem.vars) & set(tp.vars)) == 0 ):
            continue
        
        c = 0
        if tp.total_res < (elem.total_res / pagesize):
            c = (tp.total_res)/100 + tp.total_res
        else:
            c = (tp.total_res + elem.total_res) / 100
        
        if c < cost_with_another_elem or cost_with_another_elem is None:
            cost_with_another_elem = c
            
    print "cost with another elem", cost_with_another_elem
            
    if (cost_with_current_plan < cost_with_another_elem or cost_with_another_elem is None):
        print "attach", tp.sources, "to", current_plan.sources
        return True
    else:
        return False
    
    
    

