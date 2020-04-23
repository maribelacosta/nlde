from distutils.core import setup

setup(name='nlde',
      version='0.1',
      packages=['nlde', 'nlde.util', 'nlde.engine', 'nlde.policy', 'nlde.planner', 'nlde.operators'],
      url='https://github.com/nlde/nlde',
      license='GNU/GPL v2',
      author='Maribel Acosta',
      author_email='maribel.acosta@kit.edu',
      description='An Adaptive SPARQL Query Engine over RDF Data via Triple Pattern Fragments',
      requires=['ply'],
      scripts=['bin/nlde-engine'])
