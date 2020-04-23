# nLDE: Network of Linked Data Eddies

An adaptive query engine to execute SPARQL queries over [Triple Pattern Fragments](https://linkeddatafragments.org/specification/triple-pattern-fragments/). 

## Download and Install
Download or clone this repository. Execute the following command in the main folder using Python 2.7. 

```
[sudo] python setup.py install
```

## Executing a SPARQL Query
To start a proxy, execute the following command: 

```
nlde -s server -f queryfile 
```
nLDE supports several parameters: 

- `-s` or `--server`: URL of the triple pattern fragment server (required)
- `-f` or `--file`: File name of the SPARQL query 
- `-q` or `--query`: String with the SPARQL query  
- `-r` or `--results`: Format of the output results 
Options: `y` prints the query answers and overall execution time statistics, 
`n` prints only the query answers, `all`prints the query answers with timestamps and overall execution time statistics.  
- `-t` or `--timeout`: Query execution timeout in seconds  


## Example of Usage
In the following example, the query provided in the `example` folder is executed over a  
[DBpedia Triple Fragment server](http://fragments.dbpedia.org/2014/en).  

```
nlde-engine -s http://fragments.dbpedia.org/2014/en -f example/example.rq -r n
```

The nLDE answer looks as follows: 

```
{'m': '"oral, intravenous"@en', 'sm1': '"[K+].O=C[C@@H]2N3C[C@@H][C@H]3SC2C.[O-]C[C@H]2C=C/CO"@en', 'd1': 'http://dbpedia.org/resource/Amoxicillin/clavulanic_acid'}
{'m': '"Oral, intravenous"@en', 'sm1': '"O=C[C@@H]2N3C[C@@H][C@H]3SC2C.Fc1ccccc1c2nocc2CN[C@@H]3CN4[C@@H]CS[C@H]34"@en', 'd1': 'http://dbpedia.org/resource/Ampicillin/flucloxacillin'}
{'m': '"Intravenous, intramuscular"@en', 'sm1': '"O.O[C@H][C@H]2CN1C\\C=O.OC[C@@H]CSCCCC\\C=CC=O"@en', 'd1': 'http://dbpedia.org/resource/Imipenem/cilastatin'}
{'m': '"Oral, intramuscular"@en', 'sm1': '"O=C[C@@H]2N3C[C@@H][C@H]3SC2C.CN[C@@H]2C=CCNCN5CCNCC5"@en', 'd1': 'http://dbpedia.org/resource/Penimepicycline'}
{'m': 'http://dbpedia.org/resource/Intravenous_therapy', 'sm1': '"O=C[C@@H]3N4C[C@@H][C@H]4SC3C.O=C3N2[C@@H][C@@]S[C@@H]2C3"@en', 'd1': 'http://dbpedia.org/resource/Piperacillin/tazobactam'}
example.rq	0.508171081543	1.43682408333	5
```

The statistics at the bottom indicate the time when the first answer was produced, the total execution time, and the number of answers. 

## How to Cite

Maribel Acosta, Maria-Esther Vidal:
Networks of Linked Data Eddies: An Adaptive Web Query Processing Engine for RDF Data. International Semantic Web Conference (1) 2015: 111-127
