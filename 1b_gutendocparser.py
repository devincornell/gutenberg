
'''
This downloads the text and parses each gutenberg text.
Need to have already run gutenmetadb.py to make the database and populate metdata.
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

# dumb python package
from gutenberg.cleanup import strip_headers

import doctable
from gutendocsdb import GutenDocsDB

class GutenParser(doctable.DocParser):
    ''''''
    spacy_models = {
        'en': 'en_core_web_sm',
        'de': 'de_core_news_sm',
        'fr': 'fr_core_news_sm',
        'es': 'es_core_news_sm',
        'pt': 'pt_core_news_sm',
        'it': 'it_core_news_sm',
        'nl': 'nl_core_news_sm',
        'el': 'el_core_news_sm',
        'nb': 'nb_core_news_sm',
        'lt': 'lt_core_news_sm',
        #'xx': 'xx_ent_wiki_sm',
    }
    def __init__(self, dbfname, start_over=True, **kwargs):
        #self.nlp = spacy.load('xx_ent_wiki_sm') # multi-language model
        
        self.dbfname = dbfname
        self.db = GutenDocsDB(fname=self.dbfname)
        if start_over:
            self.db.update({'text':None, 'par_toks':None, 'par_ptrees':None})
            self.db.clean_col_files('text')
            self.db.clean_col_files('par_toks')
            self.db.clean_col_files('par_ptrees')
        
        dat = self.db.select(['gutenid', 'formaturi', 'language'], where=self.db['text']==None)
        self.ids = [(gid, self.url_from_uri(gid,uri), lang) for gid, uri, lang in dat]
        self.ids = [(gid, url, lang) for gid, url, lang in self.ids if url != None]
        print('Found {} of {} docs that have valid urls.'.format(len(self.ids), len(dat)))
        
    @staticmethod
    def url_from_uri(gid, uri):
        uris = [s for s in uri.split('\n') if s.endswith('.txt')]
        uris = list(sorted(uris, reverse=True))
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
            res = d.map_chunk(self.parse_guten_chunk, self.ids, 
                               self.dbfname, verbose)
        return res
            
    
    @classmethod
    def parse_guten_chunk(cls, ids, dbfname, verbose):
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
        n = len(ids)
        i = 0
        for idx, url, langs in tqdm(ids, total=n, ncols=50):
            
            # actually download file (add exception handling later)
            valid_text = True
            try:
                text = urllib.request.urlopen(url).read().decode('utf-8', errors='ignore')
                text = strip_headers(text).strip().replace('\r','')
            except urllib.error.HTTPError as e:
                print('\nThread {} threw http error {}.'.format(ids[0][0], e.code))
                valid_text = False
            
            # reload all available language models (was having memory probs)
            if i % 3000 == 0:
                #print('Thread {} loading models.'.format(ids[0][0]))
                nlps = {l:spacy.load(m) for l,m in cls.spacy_models.items()}
                #print('Thread {} done loading models.'.format(ids[0][0]))

            # decide language model to use
            flang = langs.lower().split('\n')[0].strip() if langs != '' else ''
            if valid_text and flang in nlps:
                parse_lang = flang
                nlp = nlps[flang]
                
                # actually parse texts
                parsed_pars = doctable.DocParser.parse_text_chunks(text, nlp, 
                        parse_funcs=parse_funcs, doc_transform=doc_transform, 
                        paragraph_sep='\n\n', chunk_sents=1000)
                
                # parsed pars is a list of paragraphs as lists of chunks
                par_ptrees = [[s for ch in par for s in ch['ptrees']] for par in parsed_pars]
                par_toks = [[s for ch in par for s in ch['toks']] for par in parsed_pars]
                
                # insert into db
                #gutenid, par_toks, par_ptrees, text, **kwargs
                db.insert_doc(idx, par_toks, par_ptrees, text, parse_lang)
            
                i += 1

                    
if __name__ == '__main__':
    parser = GutenParser('db/gutenberg_21.db', start_over=False)
    parser.parse_gutenberg(workers=20, verbose=False)
    #print(parser.ids[:5])
    #len(parser.ids)
    
    
    
    
    
'''
def download_nss(
	baseurl='https://raw.githubusercontent.com/devincornell/nssdocs/master/docs/',
	years = (1987, 1988, 1990, 1991, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2002, 2006, 2010, 2015, 2017)
	):
	def read_url(url):
		return urllib.request.urlopen(url).read().decode('utf-8')
	
	ftemp = baseurl+'{}.txt'
	all_texts = [read_url(ftemp.format(year)) for year in years]
	return {yr:text for yr,text in zip(years,all_texts)}
'''
    
    