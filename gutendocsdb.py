import os
#import sys
#sys.path.append('../..')
import doctable
#import ..doctable

class GutenDocsDB(doctable.DocTable):
    tabname = 'gutendocs'
    def __init__(self, fname, **kwargs):
        basename = os.path.splitext(fname)[0]
        self.schema = (
            ('idcol', 'id'),
            
            # gutenberg metadata
            ('integer', 'gutenid'),
            ('string', 'title'),
            ('string', 'author'),
            ('string', 'formaturi'),
            ('string', 'language'),
            ('string', 'rights'),
            ('string', 'subject'),
            
            # indices
            ('index', 'ind_gutenid', ['gutenid'], dict(unique=True)),
            ('index', 'ind_title', ['title', 'author'], dict(unique=True)),
            
            # keep track of language used for parsing (short version)
            ('string', 'parse_lang'),
            ('string', 'url'),
            ('index', 'ind_parse_lang', ['parse_lang']),
            
            # convenient metadata
            ('integer', 'num_pars'),
            ('integer', 'num_sents'),
            ('integer', 'num_toks'),
            
            # book text data
            ('picklefile', 'par_ptrees'),
            ('picklefile', 'par_toks'),
            ('textfile','text_body'),
            ('textfile','text_head'),
        )
        doctable.DocTable.__init__(self, fname=fname, schema=self.schema, 
            tabname=self.tabname, **kwargs)
    
    @staticmethod
    def standardize(s):
        return ' '.join(s.lower().strip().split()).title()
    
    def insert(self, row, **kwargs):
        if 'par_toks' in row:
            row['num_pars'] = len(row['par_toks'])
            row['num_sents'] = len([s for par in row['par_toks'] for s in par])
            row['num_toks'] = len([t for par in row['par_toks'] for s in par for t in s])
        
        super().insert(row, **kwargs)
    
    def insert_metadata(self, gutenid, title, author, formaturi, 
                        language, rights, subject, **kwargs):
        '''Normally this will be done first in order to populate metadata.'''
        self.insert({
            # gutenberg metadata
            'gutenid': gutenid,
            'title': self.standardize(title),
            'author': self.standardize(author),
            'formaturi': formaturi,
            'language': language,
            'rights': self.standardize(rights),
            'subject': self.standardize(subject),
        }, **kwargs)
        
    def insert_doc(self, gutenid, par_toks, par_ptrees, text, parse_lang, **kwargs):
        '''This only updates the metadata.'''
        self.update({
            # actual data payload
            'par_toks': par_toks,
            'par_ptrees': par_ptrees,
            'text': text,
            
            # info about parsed data
            'num_pars': len(par_toks),
            'num_sents': len([s for par in par_toks for s in par]),
            'num_toks': len([t for par in par_toks for s in par for t in s]),
            'parse_lang': parse_lang,
        }, where=self['gutenid'] == gutenid, **kwargs)
    

    