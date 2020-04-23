'''
Created on 18.04.2015

@author: maribelacosta
'''


def LDFParser(template, answers, var, queue, total, count, context):

    #print "answers", answers
    if template == 4:
        parseVarS(answers, var, queue, total, count, context)
    elif template == 2:
        parseVarP(answers, var, queue, total, count, context)
    elif template == 1:
        parseVarO(answers, var, queue, total, count, context)
    elif template == 6:
        parseVarSP(answers, var, queue, total, count, context)
    elif template == 5:
        parseVarSO(answers, var, queue, total, count, context)
    elif template == 3:
        parseVarPO(answers, var, queue, total, count, context)
    elif template == 0:
        parseNoVar(answers, var, queue, total, count, context)


def parseNoVar(answers, var, queue, total, count, context):

    if total == 0:
        return
    else:
        queue.put({}) 


def parseVarS(answers, var, queue, total, count, context):
    
    check_empty = str(answers)
    if "@graph" not in check_empty:
        return
    i = count
    for elem in answers:
        if "@graph" not in elem.keys():
            i = i + 1
            if i <= total:
                pos = elem["@id"].find(":")
                prefix = elem["@id"][0:pos]
                if prefix in context.keys():
                    elem["@id"] = elem["@id"].replace(prefix+":", context[prefix])
                queue.put({var[0]: elem["@id"]})
            
            
def parseVarP(answers, var, queue, total, count, context):
    
    check_empty = str(answers)
    if "@graph" not in check_empty:
        return

    for elem in answers:
        if "graph" not in elem.keys():
            answers = elem
            break

    answers = answers.keys()
    answers.remove("@id")
    
    i = count
    for elem in answers:
        i = i + 1
        if i <= total:
            pos = elem.find(":")
            prefix = elem[0:pos]
            if prefix in context.keys():
                elem = elem.replace(prefix + ":", context[prefix])
            queue.put({var[0]: elem})


def parseVarO(answers, var, queue, total, count, context):

    check_empty = str(answers)
    if "@graph" not in check_empty:
        return

    for elem in answers:
        if "@graph" not in elem.keys():
            answers = elem
            break

    del answers["@id"]
    p = answers.keys()[0]
    
    i = count
    
    if isinstance(answers[p], dict):
        elem = answers[p]
        if "@id" in elem.keys():
            o = elem["@id"]
            pos = o.find(":")
            prefix = o[0:pos]
            if prefix in context.keys():
                o = o.replace(prefix + ":", context[prefix])

        else:
            o = elem["@value"]
            if "@language" in elem.keys():
                o = '"' + o + '"' + "@" + elem["@language"]
            elif "@type" in elem.keys():
                pos = elem["@type"].find(":")
                prefix = elem["@type"][0:pos]
                if prefix in context.keys():
                    elem["@type"] = elem["@type"].replace(prefix + ":", context[prefix])

                o = '"' + o + '"' + "^^" + "<" + elem["@type"] + ">"
        
        i = i + 1
        if i <= total:
            queue.put({var[0]: o})
            
        return
   


    elif not(isinstance(answers[p], list)):
        if i <= total:
            elem = answers[p]
            if isinstance(elem, int):
                elem = '"' + str(elem) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"
            card = card + 1
            queue.put({var[0]: elem})
        return card
            
    for elem in answers[p]:
        
        if isinstance(elem, dict):
            if "@id" in elem.keys():
                o = elem["@id"]
                pos = o.find(":")
                prefix = o[0:pos]
                if prefix in context.keys():
                    o = o.replace(prefix + ":", context[prefix])

            else:
                o = elem["@value"]
                if "@language" in elem.keys():
                    o = '"' + o + '"' + "@" + elem["@language"]
                elif "@type" in elem.keys():
                    pos = elem["@type"].find(":")
                    prefix = elem["@type"][0:pos]
                    if prefix in context.keys():
                        elem["@type"] = elem["@type"].replace(prefix + ":", context[prefix])

                    o = '"' + o + '"' + "^^" + "<" + elem["@type"] + ">"

            i = i + 1
            if i <= total:
                queue.put({var[0]: o})
        else:
            if isinstance(elem, int):
                elem = '"' + str(elem) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"
            queue.put({var[0]: elem})


def parseVarSP(answers, var, queue, total, count, context):
    i = count
    for elem in answers:
        if "@graph" not in elem.keys():
            s = elem["@id"]
            pos = s.find(":")
            prefix = s[0:pos]
            if prefix in context.keys():
                s = s.replace(prefix + ":", context[prefix])

            p = elem.keys()
            p.remove("@id")
            p = p[0]
            pos = p.find(":")
            prefix = p[0:pos]
            if prefix in context.keys():
                p = p.replace(prefix + ":", context[prefix])

            if i <= total:
                queue.put({var[0]: s, var[1]: p})


def parseVarSO(answers, var, queue, total, count, context):

    check_empty = str(answers)
    if "@graph" not in check_empty:
        return
    
    i = count
    for elem in answers:

        if "@graph" not in elem.keys():
            s = elem["@id"]
            pos = s.find(":")
            prefix = s[0:pos]
            if prefix in context.keys():
                s = s.replace(prefix + ":", context[prefix])

            del elem["@id"]
            p = elem.keys()[0]
            
            if isinstance(elem[p], dict):
                if "@id" in elem[p].keys():
                    o = elem[p]["@id"]
                    pos = o.find(":")
                    prefix = o[0:pos]
                    if prefix in context.keys():
                        o = o.replace(prefix + ":", context[prefix])

                else:
                    o = elem[p]["@value"] 
                    if "@language" in elem[p].keys():
                        o = '"' + o + '"' + "@" + elem[p]["@language"]
                    elif "@type" in elem[p].keys():
                        pos = elem[p]["@type"].find(":")
                        prefix = elem[p]["@type"][0:pos]
                        if prefix in context.keys():
                            elem[p]["@type"] = elem[p]["@type"].replace(prefix + ":", context[prefix])
                        o = '"' + o + '"' + "^^" + "<" + elem[p]["@type"] + ">"
                    elif isinstance(o, int):
                        o = '"' + str(o) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"
                i = i + 1
                if i <= total:
                    queue.put({var[0]: s, var[1]: o})
                
            elif isinstance(elem[p], list):
                for oelem in elem[p]:
                    if isinstance(oelem, dict):
                        if "@id" in oelem.keys():
                            o = oelem["@id"]
                            pos = o.find(":")
                            prefix = o[0:pos]
                            if prefix in context.keys():
                                o = o.replace(prefix + ":", context[prefix])

                        else:
                            o = oelem["@value"]
                            if "@language" in oelem.keys():
                                o = '"' + o + '"' + "@" + oelem["@language"]
                            elif "@type" in oelem.keys():
                                pos = oelem["@type"].find(":")
                                prefix = oelem["@type"][0:pos]
                                if prefix in context.keys():
                                    oelem["@type"] = oelem["@type"].replace(prefix + ":", context[prefix])
                                o = '"' + o + '"' + "^^" + "<" + oelem["@type"] + ">"
                            elif isinstance(o, int):
                                o = '"' + str(o) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"

                        i = i + 1
                        if i <= total:
                            queue.put({var[0]: s, var[1]: o})
                    else:
                        i = i + 1
                        if i <= total:
                            if isinstance(oelem, int):
                                oelem = '"' + str(oelem) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"
                            queue.put({var[0]: s, var[1]: oelem})
            
            else:
                i = i + 1
                if i <= total:
                    if isinstance(elem[p], int):
                        elem[p] = '"' + str(elem[p]) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"

                    queue.put({var[0]: s, var[1]: elem[p]})
                    
                    
def parseVarPO(answers, var, queue, total, count, context):

    for elem in answers:
        if "@graph" not in elem.keys():
            answers = elem
            break

    i = count
    for elem in answers.keys():
        
        p = elem
        if isinstance(answers[p], dict):

            if "@id" in answers[p].keys():
                o = answers[p]["@id"]
                pos = o.find (":")
                prefix = o[0:pos]
                if prefix in context.keys():
                    o = o.replace(prefix + ":", context[prefix])

            else:
                o = answers[p]["@value"]
                if "@language" in answers[p].keys():
                    o = '"' + o + '"' + "@" + answers[p]["@language"]
                elif "@type" in answers[p].keys():
                    pos = answers[p]["@type"].find(":")
                    prefix = answers[p]["@type"][0:pos]
                    if prefix in context.keys():
                        answers[p]["@type"] = answers[p]["@type"].replace(prefix + ":", context[prefix])
                    o = '"' + o + '"' + "^^" + "<" + answers[p]["@type"] + ">"
                elif isinstance(o, int):
                    o = '"' + str(o) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"

            i = i + 1
            if i <= total:
                queue.put({var[0]: p, var[1]: o})

        elif isinstance(answers[p], list):
            for oelem in answers[p]:
                if isinstance(oelem, dict):
                    if "@id" in oelem.keys():
                        o = oelem["@id"]
                        pos = o.find(":")
                        prefix = o[0:pos]
                        if prefix in context.keys ():
                            o = o.replace(prefix + ":", context[prefix])

                    else:
                        o = oelem["@value"]
                        if "@language" in oelem.keys():
                            o = '"' + o + '"' + "@" + oelem["@language"]
                        elif "@type" in oelem.keys():
                            pos = oelem["@type"].find(":")
                            prefix = oelem["@type"][0:pos]
                            if prefix in context.keys():
                                oelem["@type"] = oelem["@type"].replace (prefix + ":", context[prefix])
                            o = '"' + o + '"' + "^^" + "<" + oelem["@type"] + ">"
                        elif isinstance(o, int):
                            o = '"' + str(o) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"
                    i = i + 1

                    if i <= total:
                        queue.put({var[0]: p, var[1]: o})
                else:
                    i = i + 1
                    if i <= total:
                        queue.put({var[0]: p, var[1]: oelem})
        else:
            i = i + 1


def isliteral(s):
    if (isinstance(s, str)) and (len(s) > 0) and (s[0] == '"'):
        return True
    else:
        return False


def replace_prefix(uri, prefix, path):
    return "<" + uri.replace(prefix+":", path) + ">"

