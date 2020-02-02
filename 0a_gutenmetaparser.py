

'''
Parses metdata using the gutenberg python package and stores in database.
Actual doc information is inserted elsewhere - but this needs to be done first.
Separate because the gutenberg python package is pretty flaky.
'''



import signal # for timeout on the gutenberg package metdata read
import math
from glob import glob
import os
import zipfile
import re
from tqdm import tqdm
import spacy
from pprint import pprint

# db to store metadata
from gutendocdb import GutenDocDB

# dumb python gutenberg package stuff
# https://pypi.org/project/Gutenberg/
from gutenberg.acquire.metadata import SleepycatMetadataCache
from gutenberg.acquire import set_metadata_cache
from gutenberg.query import get_etexts
from gutenberg.query import get_metadata
from gutenberg.acquire import load_etext
from gutenberg.cleanup import strip_headers
from gutenberg.query import list_supported_metadatas
import gutenberg


#import sys
#sys.path.append('...')
import doctable

class GutenMetaParser(doctable.DocParser):
    '''For storing document metdata.'''
    
    def __init__(self, dbfname, cache_db, start_over=True, **kwargs):
        self.nlp = spacy.load('en')
        
        self.dbfname = dbfname
        self.cache_db = cache_db
        self.db = GutenDocDB(fname=self.dbfname)
        
        # for whatever reason I think this is currently the last book
        last_id = 61041
        if start_over:
            prev_ids = set(self.db.select('gutenid'))
            self.ids = [i for i in range(last_id) if i not in prev_ids]
        else:
            self.db.delete()
            self.ids = list(range(last_id))
        
        
    def parse_gutenberg(self, workers=None, verbose=False):
        '''Parse and store nss docs into a doctable.
        Args:
            years (list): years to request from the nss corpus
            dbfname (str): fname for DocTable to initialize in each process.
            workers (int or None): number of processes to create for parsing.
        '''
        
        with doctable.Distribute(workers) as d:
            res = d.map_chunk(self.parse_guten_chunk, self.ids, 
                              self.dbfname, self.cache_db, verbose)
        return res
    
    @classmethod
    def parse_guten_chunk(cls, ids, dbfname, cache_db, verbose):
        '''Runs in separate process for each chunk of nss docs.
        Description: parse each fname and store into database.
        Args:
            fnames (list<str>): list of filenames to read
            dbfname (str): filename of database to write into
            verbose (bool): print progress
        '''
        
        # create a new database connection
        db = GutenDocDB(fname=dbfname)
        
        # set up gutenberg cache for python package
        cache = SleepycatMetadataCache(cache_db)
        set_metadata_cache(cache)
        
                
        # loop through each potential document
        if verbose: print('starting to get metadata (if takes a long time, kill and restart)')
        
        # loop through each doc index
        n = len(ids)
        for i, idx in tqdm(enumerate(ids), total=n, ncols=50):
            
            #if verbose: print('\n--> holding for lang ({})'.format(ids[0]))
            language = get_metadata('language', idx)
            #if verbose: print('\n--> got lang:', language, '({})'.format(ids[0]))

            title = '\n'.join(get_metadata('title', idx))
            author = '\n'.join(get_metadata('author', idx))
            subject = '\n'.join(get_metadata('subject', idx))
            language = '\n'.join(get_metadata('language', idx))
            rights = '\n'.join(get_metadata('rights', idx))
            formaturi = '\n'.join(get_metadata('formaturi', idx))
            
            db.insert_metadata(idx, title, author, formaturi, 
                language, rights, subject, ifnotunique='replace')

                
if __name__ == '__main__':
    cachename = 'gutenberg_python_package_db/guten_ache5.db'
    dbname = 'db/gutenberg_21.db'
    parser = GutenMetaParser(dbname, cachename, start_over=False)
    parser.parse_gutenberg(workers=1, verbose=True)
    









