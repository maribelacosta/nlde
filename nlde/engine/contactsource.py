"""
Created on Mar 23, 2015

@author: Maribel Acosta
"""
from nlde.util.ldfparser import LDFParser
import httplib
import urllib

user_agent = "nLDE 0.1"
accept = "application/json"


def get_metadata_ldf(server, query):

    # Extract server information.
    referer = server
    server = server.split("http://")[1]
    (server, path) = server.split("/", 1)
    host_port = server.split(":")
    port = 80 if len(host_port) == 1 else host_port[1]

    # Prepare parameters and header of request
    params = urllib.urlencode({"subject": query.subject.value,
                               "predicate": query.predicate.value,
                               "object": query.object.value})
    headers = {"User-Agent": user_agent,
               "Referer": referer,
               "Host": server,
               "Accept": accept}

    # Establish connection and get response from server.
    conn = httplib.HTTPConnection(server)
    # conn.set_debuglevel(1)
    conn.request("GET", "/" + path + "?" + params, None, headers)
    response = conn.getresponse()
    total = -1

    # Successfully contacted the server.
    if response.status == httplib.OK:
        res = response.read()
        pos1 = res.find('"void:triples": ')
        pos2 = res[pos1:].find(",")
        total = int(res[pos1 + len('"void:triples": '):pos1 + pos2])

    else:
        # TODO: Inform the user that the server could not be contacted.
        pass

    # Return estimated number of triples in fragment.
    return total


def contact_ldf_server(server, query, queue):

    # Extract server information.
    referer = server
    server = server.split("http://")[1]
    (server, path) = server.split("/", 1)
    host_port = server.split(":")
    port = 80 if len(host_port) == 1 else host_port[1]

    template = 0
    qvars = []

    # Build query for request:
    # extract subject, predicate, object (value).

    # Extract subject.
    subject = query.subject
    if subject.isvariable():
        template = template | 4
        qvars.append(subject.get_variable())

    # Extract predicate.
    predicate = query.predicate
    if predicate.isvariable():
        template = template | 2
        qvars.append(predicate.get_variable())

    # Extract object (value).
    value = query.object
    if value.isvariable():
        template = template | 1
        qvars.append(value.get_variable())

    # Literal in subject or predicate position: NOT a valid triple pattern.
    if subject.isliteral() or predicate.isliteral():
        queue.put("EOF")
        return

    # Prepare parameters of request.
    params = urllib.urlencode({"subject": subject.value,
                               "predicate":  predicate.value,
                               "object": value.value})
    headers = {"User-Agent": user_agent,
               "Referer": referer,
               "Host": server,
               "Accept": accept}

    # Pagination settings.
    pagesize = 100 # TODO: This should be extracted from the metadata.
    count = -1
    total = 0
    page = 1

    # Paginate.
    while count < total:

        # Establish connection and get response from server.
        conn = httplib.HTTPConnection(server)
        # conn.set_debuglevel(1)
        conn.request("GET", "/" + path + "?" + params, None, headers)
        response = conn.getresponse()

        if response.status == httplib.OK:

            # Read the response.
            res = response.read()

            # Get total number of triples in the fragment.
            pos1 = res.find('"void:triples": ')
            pos2 = res[pos1:].find(",")
            total = int(res[pos1+len('"void:triples": '):pos1+pos2])

            # Parse response.
            myres = eval(res)

            LDFParser(template, myres["@graph"], qvars, queue, total, count, myres["@context"])

            # Update stats and prepare next request.
            count = count + pagesize
            page = page + 1
            params = urllib.urlencode({'subject': subject.value, 'predicate':  predicate.value, "object": value, "page": page})

        else:
            # TODO: Inform user that source could not be contacted.
            break

    # Close the queue.
    queue.put("EOF")

