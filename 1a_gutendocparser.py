
'''
This uses the .json metadata file to populate the database directly.
'''

import urllib.request
import urllib.error
import math
from glob import glob
import os
import zipfile
import re
from tqdm import tqdm
import spacy
from pprint import pprint
import json
import time
import numpy as np

# dumb python package
from gutenberg.cleanup import strip_headers

import doctable
from gutendocsdb import GutenDocsDB

class GutenParser(doctable.DocParser):
    ''''''
    spacy_models = {
        'en': 'en_core_web_sm',
        #'de': 'de_core_news_sm',
        #'fr': 'fr_core_news_sm',
        #'es': 'es_core_news_sm',
        #'pt': 'pt_core_news_sm',
        #'it': 'it_core_news_sm',
        #'nl': 'nl_core_news_sm',
        #'el': 'el_core_news_sm',
        #'nb': 'nb_core_news_sm',
        #'lt': 'lt_core_news_sm',
        #'xx': 'xx_ent_wiki_sm',
    }
    def __init__(self, dbfname, mdfname, start_over=True, **kwargs):
        #self.nlp = spacy.load('xx_ent_wiki_sm') # multi-language model
        # load json data
        
        self.dbfname = dbfname
        self.db = GutenDocsDB(fname=self.dbfname)
        if start_over:
            #self.db.update({'text':None, 'par_toks':None, 'par_ptrees':None})
            self.db.delete()
            self.db.clean_col_files('text')
            self.db.clean_col_files('par_toks')
            self.db.clean_col_files('par_ptrees')
        
        # load and format metadata
        with open(mdfname, 'r') as f:
            meta = json.load(f)
        meta = [self.format_metadata(gid,md) for gid,md in meta.items()]
        
        #dat = self.db.select(['gutenid', 'formaturi', 'language'], where=self.db['text']==None)
        #self.ids = [(gid, self.url_from_uri(gid,uri), lang) for gid, uri, lang in dat]
        #self.ids = [(gid, url, lang) for gid, url, lang in self.ids if url != None]
        #self.meta = [(gid, dat) for gid, dat in meta.items() if gid not in finished_ids]
        finished_ids = set(self.db.select('gutenid'))
        self.metadata = [md for md in meta if md['gutenid'] not in finished_ids]
        print('Found {} of {} docs that have valid urls.'.format(len(self.metadata), len(meta)))
        
    @classmethod
    def format_metadata(cls, gid, md):
        dat = dict()
        dat['gutenid'] = int(gid)
        dat['author'] = cls.format_entry(md['author']).title()
        dat['formaturi'] = '\n'.join([d.strip() for d in md['formaturi']])
        dat['language'] = cls.format_entry(md['language'])
        dat['rights'] = cls.format_entry(md['rights']).title()
        dat['subject'] = cls.format_entry(md['subject']).title()
        dat['title'] = cls.format_entry(md['title']).title()
        dat['url'] = cls.url_from_uri(md['formaturi'])
        
        return dat
        
        
    @staticmethod
    def format_entry(lines):
        '''Given a list of entries, cleans formatting of a row.'''
        return '\n'.join([' '.join(line.strip().lower().split()) for line in lines])
        
    @staticmethod
    def url_from_uri(uris):
        uris = [s for s in uris if s.endswith('.txt')]
        uris = list(sorted(uris, reverse=False))
        # find first because later versions have dashes; i.e.:
        # http://www.gutenberg.org/files/13/13.txt vs
        # http://www.gutenberg.org/files/13/13-0.txt (second one is better)
        if len(uris) > 0:
            return uris[0]
        else:
            #raise ValueError(f'Couldn\'t find text file for {gid}: {uris}')
            return None
        
    def parse_gutenberg(self, workers=None, verbose=False):
        '''Parse and store nss docs into a doctable.
        Args:
            years (list): years to request from the nss corpus
            dbfname (str): fname for DocTable to initialize in each process.
            workers (int or None): number of processes to create for parsing.
        '''
        with doctable.Distribute(workers) as d:
            res = d.map_chunk(self.parse_guten_chunk, self.metadata, 
                               self.dbfname, verbose)
        return res
            
    
    @classmethod
    def parse_guten_chunk(cls, metadata, dbfname, verbose):
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
        
        # define parsing functions and regex
        #re_start = re.compile('\n\*\*\*.*START OF .* GUTENBERG .*\n')
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
        doc_transform = lambda doc: doctable.DocParser.apply_doc_transform(doc, merge_ents=True)
        
        # loop through each potential document
        n = len(metadata)
        i = 0
        for md in tqdm(metadata, total=n, ncols=50):
            row = md # save all metadata columns
            
            # reload all available language models (was having memory probs)
            if i % 3000 == 0:
                #print('\nThread {} loading models.'.format(metadata[0]['gutenid']))
                nlps = {l:spacy.load(m) for l,m in cls.spacy_models.items()}
                #print('Thread {} done loading models.'.format(ids[0][0]))
            

            # actually download file (add exception handling later)
            valid_text = True
            if row['url'] is None:
                valid_text = False
            else:
                try:
                    start = time.time()
                    text_og = urllib.request.urlopen(row['url']).read().decode('utf-8', errors='ignore')
                    text = strip_headers(text_og).strip().replace('\r','')
                    #print('downloaded in {:.3f} sec.'.format(time.time()-start))

                    row['text_body'] = text # save text
                    row['text_head'] = text_og[:len(text_og)-len(text)]
                except urllib.error.HTTPError as e:
                    print('\nThread X threw http error {}: {}'.format(e.code, row['url']))
                    #raise
                    valid_text = False
                #except AttributeError:
                #    print('\nThread X couldn\'t find url. ({})'.format(row['url']))
                    #raise
                #    valid_text = False
                except urllib.error.URLError:
                    print('\nURL error. ({})'.format(row['url']))
                    #raise
                    time.sleep(np.random.poisson(60))
                    valid_text = False
            
                # decide language model to use
                first_lang = row['language'].split()[0] if row['language'] != '' else ''
                
            
            if not valid_text or first_lang not in nlps:
                #ll = '/'.join(r.strip().lower() for r in row['language'])
                #print('\nlanguage {} was not found ({})'.format(first_lang, row['language']))
                pass
                
            elif valid_text: # first_lang is good and the text is valid
                parse_lang = first_lang
                nlp = nlps[first_lang]
                row['parse_lang'] = first_lang # record parser that was used

                # actually parse texts
                parse_successful = True
                try:
                    parsed_pars = doctable.DocParser.parse_text_chunks(text, nlp, 
                            parse_funcs=parse_funcs, doc_transform=doc_transform, 
                            paragraph_sep='\n\n', chunk_sents=1000)
                    #print('finished parsing')
                except ValueError:
                    #raise
                    print('\nproblem parsing paragraphs')
                    parse_successful = False
                
                if parse_successful:
                    # parsed pars is a list of paragraphs as lists of chunks
                    row['par_ptrees'] = [[s for ch in par for s in ch['ptrees']] for par in parsed_pars]
                    row['par_toks'] = [[s for ch in par for s in ch['toks']] for par in parsed_pars]
                    
                    #row['num_pars'] = len(row['par_toks']),
                    #row['num_sents'] = len([s for par in row['par_toks'] for s in par]),
                    #row['num_toks'] = len([t for par in row['par_toks'] for s in par for t in s]),
                    
                    i += 1
                
                # insert into db
                #gutenid, par_toks, par_ptrees, text, **kwargs
            #if 'par_ptrees' not in row:
            #    print('\noh dammit shit')
            #print({k:type(v) for k,v in row.items()})
            db.insert(row, ifnotunique='replace')
            
                

                    
if __name__ == '__main__':
    mdname = 'metadata/gutenberg-metadata.json'
    parser = GutenParser('db/gutenberg_00.db', mdname, start_over=False)
    parser.parse_gutenberg(workers=25, verbose=False)
    #print(parser.ids[:5])
    #len(parser.ids)
    
    
    
    
    
    