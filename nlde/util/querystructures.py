class Query(object):
    def __init__(self, prefixes, projection, where, distinct, order_by=[], limit="", offset=""):
        self.prefixes = prefixes
        self.projection = projection
        self.where = where
        self.distinct = distinct
        self.order_by = order_by
        self.limit = limit
        self.offset = offset
        self.form = "SELECT"

    def __repr__(self):

        # Defaults.
        distinct = ""
        variables = "*"
        order_by = ""
        limit = ""
        offset = ""

        # Prefixes.
        prefixes = self.print_prefixes()

        # Distinct.
        if self.distinct:
            distinct = "DISTINCT "

        # Projection.
        if len(self.projection) > 0:
            variables = " ".join(list(map(str, self.projection)))

        # Where clause.
        where = "{\n" + str(self.where) + "}"

        if len(self.order_by) > 0:
            order_by = "ORDER BY" + " ".join(self.order_by)

        if self.limit:
            limit = "LIMIT " + self.limit

        if self.offset:
            offset = "OFFSET " + self.offset

        return "{0}{1} {2}{3}\nWHERE {4}{5}\n{6}\n{7}\n".format(
            prefixes, self.form, distinct, variables, where, order_by, limit, offset)

    def print_prefixes(self):

        prefixes_str = ""

        for p in self.prefixes:
            prefixes_str = prefixes_str + "prefix " + p + "\n"

        return prefixes_str


class GroupGraphPattern(object):

    def __init__(self, left, right=None, join=False, union=False, optional=False):
        self.left = left
        self.right = right
        self.join = join
        self.union = union
        self.optional = optional

    def __repr__(self):

        operator = "."

        if self.union:
            operator = "UNION"
            left = "{" + str(self.left) + "}\n"
        elif self.optional:
            operator = "OPTIONAL"
            left = str(self.left) + "\n"

        if self.right:
            return left + operator + "\n{" + str(self.right) + "}\n"
        else:
            return str(self.left)


class TriplesBlock(object):
    def __init__(self, triple_patterns):
        self.triple_patterns = triple_patterns

    def __repr__(self):
        return "\n".join(list(map(str, self.triple_patterns)))


class TriplePattern(object):
    def __init__(self, s, p, o):
        self.subject = s
        self.predicate = p
        self.object = o

    def __repr__(self):
        return str(self.subject) + " " + str(self.predicate) + " " + str(self.object) + " . "

    def get_variables(self):
        variables = []

        if not self.subject.isconstant:
            variables.append(self.subject.get_variable())

        if not self.predicate.isconstant:
            variables.append(self.predicate.get_variable())

        if not self.object.isconstant:
            variables.append(self.object.get_variable())

        return set(variables)


class Argument(object):
    def __init__(self, value, isconstant=False):
        self.value = value
        self.isconstant = self.isuri() or self.isliteral()

    def __repr__(self):
        return str(self.value)

    def isuri(self):
        if self.value[0] == "<":
            return True
        else:
            return False

    def isvariable(self):
        if (self.value[0] == "?") or (self.value[0] == "$"):
            return True
        else:
            return False

    def isbnode(self):
        if self.value[0] == "_":
            return True
        else:
            return False

    def isliteral(self):
        if self.value[0] == '"':
            return True
        else:
            return False

    def get_variable(self):
        if self.isvariable():
            return self.value[1:]

class Filter(object):
    def __init__(self):
        pass


class Expression(object):
    def __init__(self):
        pass