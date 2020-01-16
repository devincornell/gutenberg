import math
from glob import glob
import os
import zipfile
import re
from tqdm import tqdm
import spacy
from gutendocsdb import GutenDocsDB
from pprint import pprint

from gutenberg.acquire.metadata import SleepycatMetadataCache
#from gutenberg.acquire.metadata import SqliteMetadataCache
from gutenberg.acquire import set_metadata_cache
from gutenberg.query import get_etexts
from gutenberg.query import get_metadata
from gutenberg.acquire import load_etext
from gutenberg.cleanup import strip_headers
from gutenberg.query import list_supported_metadatas
import gutenberg


import sys
sys.path.append('...')
import doctable

class GutenParser(doctable.DocParser):
    ''''''
    
    def __init__(self, dbfname, **kwargs):
        self.nlp = spacy.load('en')
        
        self.dbfname = dbfname
        self.db = GutenDocsDB(fname=self.dbfname)
        prev_ids = self.db.select('gutenid')
        
        # for whatever reason I think this is currently the last book
        self.ids = [i for i in range(61041) if i not in prev_ids]
        
        
    def parse_gutenberg(self, workers=None, verbose=False):
        '''Parse and store nss docs into a doctable.
        Args:
            years (list): years to request from the nss corpus
            dbfname (str): fname for DocTable to initialize in each process.
            workers (int or None): number of processes to create for parsing.
        '''
        
        self.db.clean_col_files('text')
        self.db.clean_col_files('par_toks')
        self.db.clean_col_files('par_ptrees')
        
        with doctable.Distribute(workers) as d:
            res = d.map_chunk(self.parse_guten_chunk, self.ids, self.nlp, 
                               self.dbfname, verbose)
        return res
    
    @classmethod
    def parse_guten_chunk(cls, ids, nlp, dbfname, verbose):
        '''Runs in separate process for each chunk of nss docs.
        Description: parse each fname and store into database
        Args:
            fnames (list<str>): list of filenames to read
            nlp (spacy parser): parser
            dbfname (str): filename of database to write into
            verbose (bool): print progress
        '''
        
        # create a new database connection
        db = GutenDocsDB(fname=dbfname)
        
        # set up gutenberg cache
        cache = SleepycatMetadataCache('guten_cache2.db')
        set_metadata_cache(cache)
        
        # define parsing functions and regex
        re_start = re.compile('\n\*\*\*.*START OF .* GUTENBERG .*\n')
        use_tok = lambda tok: cls.use_tok(tok, filter_whitespace=True)
        parse_tok = lambda tok: cls.parse_tok(tok, num_replacement='XXNUMXX', format_ents=True)
        tokenize = lambda doc: cls.tokenize_doc(doc, split_sents=True,
                                parse_tok_func=parse_tok, use_tok_func=use_tok)
        parsetrees = lambda doc: cls.get_parsetrees(doc, merge_ents=False, 
                                parse_tok_func=parse_tok)
        parse_funcs = {
            'toks': tokenize,
            'ptrees': parsetrees,
        }
        doc_tform = lambda doc: doctable.DocParser.apply_doc_transform(doc, merge_ents=True)
        
        # use loading bar or not
        #if verbose:
        #    n = len(ids)
        #    iter = tqdm(enumerate(ids), total=n, ncols=50)
        #else:
        #    iter = enumerate(ids)
        
        
        # loop through each potential document
        n = len(ids)
        for i, idx in tqdm(enumerate(ids), total=n, ncols=50):
            
            if i % 100 == 0: # renew parser
                nlp = spacy.load('en')
            
            if verbose: print('\n--> holding for lang ({})'.format(ids[0]))
            language = get_metadata('language', idx)
            if verbose: print('\n--> got lang:', language, '({})'.format(ids[0]))
            if 'en' in language:
                valid_etext = True
                try:
                    text = strip_headers(load_etext(i)).strip().replace('\r','')
                except gutenberg._domain_model.exceptions.InvalidEtextIdException:
                    valid_etext = False
                except gutenberg._domain_model.exceptions.UnknownDownloadUriException:
                    valid_etext = False
                except EOFError:
                    valid_etext = False # messed up unzipping text file?

                if valid_etext:
                    title = '\n'.join(get_metadata('title', idx))
                    author = '\n'.join(get_metadata('author', idx))
                    subject = '\n'.join(get_metadata('subject', idx))
                    language = '\n'.join(get_metadata('language', idx))
                    rights = '\n'.join(get_metadata('rights', idx))
                    formaturi = '\n'.join(get_metadata('formaturi', idx))
                    
                    parsed_pars = doctable.DocParser.parse_text_chunks(text, nlp, 
                            parse_funcs=parse_funcs, doc_transform=doc_tform, 
                            paragraph_sep='\n\n', chunk_sents=1000)
                    
                    # parsed pars is a list of paragraphs as lists of chunks
                    par_ptrees = [[s for ch in par for s in ch['ptrees']] for par in parsed_pars]
                    par_toks = [[s for ch in par for s in ch['toks']] for par in parsed_pars]
                    
                    #pprint(par_ptrees)
                    #print(text[-5000:])
                    #exit()
                    db.insert_doc(idx, par_toks, par_ptrees, text, title, author, formaturi, 
                        language, rights, subject, ifnotunique='replace')
                
if __name__ == '__main__':
    parser = GutenParser('db/gutenberg_17.db')
    parser.parse_gutenberg(workers=20, verbose=False)
    