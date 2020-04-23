import parser.sparqlparser_new
import parser.sparqlparser

if __name__ == '__main__':
    query = "SELECT * WHERE { ?s ?p ?o .\n ?s ?p ?o1 .\n ?o ?p ?x .\n ?o ?x \"lala\" . } OFFSET 5 LIMIT 10"

    q = parser.sparqlparser_new.parse(query)
    print q.where.left.triple_patterns

    for tp in q.where.left.triple_patterns:
        print tp, tp.get_variables()

    t = q.where.left.triple_patterns[3]
    t.subject.isuri()
    print t, "subject uri", t.subject.isuri()
    print t, "subject var", t.subject.isvariable(), t.subject.get_variable()
    print t.object.isliteral()
    print str(q)

    x = parser.sparqlparser.SPARQLParser(query, "lala")
    print x.getSubQueries("")

