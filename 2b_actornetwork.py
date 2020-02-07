

from gutendocsdb import GutenDocsDB
import networkx as nx
from tqdm import tqdm


def subj_verb_obj(doc):
    nounverbs = list()
    for tok in doc:
        if tok.pos == 'VERB':
            rel = (child_dep(tok,'nsubj'), tok, child_dep(tok,'dobj'))
            nounverbs.append(rel)
    return nounverbs

def child_dep(tok, dep): # gets first child where child.dep_==dep.
    for c in tok.childs:
        if c.dep == dep:
            return c
    return None

def add_node_rel(G, utok, vtok, words=None):
    if utok is None or vtok is None:
        return
    if words is not None and not (utok.tok in words or vtok.tok in words):
        return
    #print(utok, vtok)
    
    # add nodes with zero count
    if utok.tok not in G.nodes():
        G.add_node(utok.tok, typ=utok.pos, ent=utok.ent, ct=0)
    if vtok.tok not in G.nodes():
        G.add_node(vtok.tok, typ=vtok.pos, ent=utok.ent, ct=0)
    
    # add edge with zero weight if it doesn't exist
    u,v = utok.tok, vtok.tok
    if (u,v) not in G.edges():
        G.add_edge(u, v, weight=0)
    
    # increment node counts and edge count
    G.nodes[u]['ct'] += 1
    G.nodes[v]['ct'] += 1
    G[u][v]['weight'] += 1

def actor_network(sents, min_node_ct=None):
    G = nx.DiGraph()
    for sent in sents:
        for subj, verb, obj in subj_verb_obj(sent):
            add_node_rel(G, subj, verb, words=['he', 'she', 'they'])
            add_node_rel(G, verb, obj)
            
    # remove nodes that don't meet minimum count threshold
    if min_node_ct is not None:
        edge_cutoff(G, min_node_ct)
            
    return G

def edge_cutoff(G, cutoff):
    rm_nodes = list()
    for n in G.nodes():
        if G.nodes[n]['ct'] < cutoff:
            rm_nodes.append(n)
    G.remove_nodes_from(rm_nodes)
    return G


def merge_graphs(Gall, Gnew):
    #print('motherfucker')
    for n in Gnew.nodes():
        if not n in Gall.nodes():
            Gall.add_node(n, typ=Gnew.nodes[n]['typ'], ent=Gnew.nodes[n]['ent'], ct=0)
        Gall.nodes[n]['ct'] += 1
    #print('damn')
    for u,v,d in Gnew.edges(data=True):
        if (u,v) not in Gall.edges():# or (v,u) in Gall.edges()):
            Gall.add_edge(u,v, weight=d['weight'])
        Gall.edges[(u,v)]['weight'] += 1
        
    #print('god dammit')
    return Gall



def save_graph(Gall, save_file):
    edge_cutoff(Gall, 2)
    print('\n', len(Gall.nodes()), len(Gall.edges()))
    nx.write_gexf(Gall, save_file)


if __name__ == '__main__':
    db = GutenDocsDB('db/gutenberg_21.db')
    save_file = 'networks/actor3_big.gexf'
    try:
        Gall = nx.DiGraph()
        select = db.select_chunk(['author','par_ptrees'], limit=100000000, chunksize=1)
        for auth, ptrees in tqdm(select, total=db.count(), ncols=50):
            if ptrees is not None:
                sents  = [sent for par in ptrees for sent in par]
                print(len(sents), end='', flush=True)
                if len(sents) > 0:
                    Gnew = actor_network(sents)
                    merge_graphs(Gall, Gnew)
                    print('. ', end='', flush=True)
    
    except:
        #raise
        save_graph(Gall, save_file)
        print('failed.')
        raise
    
    save_graph(Gall, save_file)
    print('finished')
    