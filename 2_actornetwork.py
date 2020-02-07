

from gutendocsdb import GutenDocsDB
from subjverbobjdb import SubjVerbObjDB
import networkx as nx
from tqdm import tqdm
import doctable

def subj_verb_obj(doc):
    subjverbobj = list()
    for tok in doc:
        if tok.pos == 'VERB':
            rel = (child_dep(tok,'nsubj'), tok, child_dep(tok,'dobj'))
            subjverbobj.append(rel)
    return subjverbobj

def child_dep(tok, dep): # gets first child where child.dep_==dep.
    for c in tok.childs:
        if c.dep == dep:
            return c
    return None




def insert_triplets(ids, docdb_fname, tripdb_fname):
    
    gutendb = GutenDocsDB(docdb_fname)
    tripdb = SubjVerbObjDB(tripdb_fname)
    
    columns = ['id', 'gutenid', 'par_ptrees', 'parse_lang']
    select = gutendb.select_chunk(columns, chunksize=1, where=gutendb['gutenid'].in_(ids), limit=10000000000)
    for docid, gid, ptrees, lang in tqdm(select, total=len(ids), ncols=50):
        if ptrees is not None:
            sents  = [sent for par in ptrees for sent in par]
            #print(len(sents), end='', flush=True)
            for sent in sents:
                for subj, verb, obj in subj_verb_obj(sent):
                    if subj is not None or obj is not None:
                        tripdb.insert({
                            'subject': subj.tok if subj is not None else None,
                            'verb': verb.tok,
                            'object': obj.tok if obj is not None else None,

                            'docid': docid,
                            'gutenid': gid,
                            'language': lang,
                        }, ifnotunique='replace')


if __name__ == '__main__':
    docdb_fname = 'db/gutenberg_24.db'
    tripdb_fname = 'db/actornet1.db'
    
    gutendb = GutenDocsDB(docdb_fname)
    tripdb = SubjVerbObjDB(tripdb_fname)
    all_ids = set(gutendb.select('gutenid'))
    finished_ids = set(tripdb.select('gutenid'))
    ids = list(all_ids-finished_ids)
    
    print('parsing {} docs (of {} total)'.format(len(ids), len(all_ids)))
    
    with doctable.Distribute(50, override_maxcores=True) as d:
        d.map_chunk(insert_triplets, ids, docdb_fname, tripdb_fname)
    
    #insert_triplets(gutendb, tripdb)
    print('finished')
    
    