"""
Code fragments from Lesson 3.3 `Working with rdflib'
For more information about rdflib and SPARQL: 
https://rdflib.readthedocs.io/en/stable/
http://rdflib.readthedocs.io/en/stable/intro_to_sparql.html
https://www.w3.org/TR/rdf-sparql-query/
"""

# import Graph object from rdflib package
from rdflib import Graph

# parse the file supplied with this lesson
g = Graph().parse(source="pg30291.rdf")

# take the length of the file (number of statements)
len(g)

# you might already have data in a Python object...
f = open("pg30291.rdf")
text = f.read()
f.close()
text[0:40]

# ...in which case, use parse method to parse data into Graph object
h = Graph().parse(data=text)
len(h)

# use simple query interface, RDF, to perform pattern matching on triples
from rdflib.namespace import RDF
for s, p, o in g.triples((None,RDF.type, None)):
    print("%s is of type %s" % (s,o))

# SPARQL equivalent of above pattern match
q = """
	PREFIX pgterms: <http://www.gutenberg.org/2009/pgterms/>

	SELECT ?s ?o WHERE {
		?s rdf:type ?o .
	}
	"""
# execute query method of the graph object
for s, o in g.query(q):
	print("%s is of type %s" % (s, o))

# get the book's author, as well as the book id
q = """
	PREFIX pgterms: <http://www.gutenberg.org/2009/pgterms/>

	SELECT DISTINCT ?book ?author WHERE {
		?book rdf:type pgterms:ebook .
		?book dcterms:creator ?agent .
		?agent pgterms:name ?author .
	}
	"""
for book, author in g.query(q):
	print("%s wrote %s" % (author, book))

# serialize existing graph in turtle format (you may find it easier to read)
print(g.serialize(format="turtle"))



