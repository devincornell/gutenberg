import os
import sys
sys.path.append('../..')
import doctable
#import ..doctable

class GutenDocsDB(doctable.DocTable):
    tabname = 'gutendocs'
    def __init__(self, fname, **kwargs):
        basename = os.path.splitext(fname)[0]
        self.schema = (
            ('idcol', 'id'),
            #('string', 'fname', dict(nullable=False)),
            ('integer', 'gutenid'),
            ('string', 'title', {'unique':True}),
            ('string', 'author'),
            ('string', 'formaturi'),
            ('string', 'language'),
            ('string', 'rights'),
            ('string', 'subject'),
            
            # book text data
            ('picklefile', 'par_ptrees', {}, {'fpath': basename+'_ptrees'}),
            ('picklefile', 'par_toks', {}, {'fpath': basename+'_toks'}),
            ('textfile','text', {}, {'fpath': basename+'_texts'}),
            
            # convenient metadata
            ('integer', 'num_pars'),
            ('integer', 'num_sents'),
            ('integer', 'num_toks'),
            #('index', 'ind_fname', ['fname'], dict(unique=True)),
        )
        doctable.DocTable.__init__(self, fname=fname, schema=self.schema, 
            tabname=self.tabname, **kwargs)
        
    def insert_doc(self, gutenid, par_toks, par_ptrees, full_text, title, 
        author, formaturi, language, rights, subject, **kwargs):
        self.insert({
            # gutenberg metadata
            'gutenid': gutenid,
            'title': title,
            'author': author,
            'formaturi': formaturi,
            'language': language,
            'rights': rights,
            'subject': subject,
            
            # actual data payload
            'par_toks': par_toks,
            'par_ptrees': par_ptrees,
            'text': full_text,
            
            # data info
            'num_pars': len(par_toks),
            'num_sents': len([s for par in par_toks for s in par]),
            'num_toks': len([t for par in par_toks for s in par for t in s]),
        }, **kwargs)
        
    def select_doc_toks():
        par_toks = self.select(['id', 'par_toks'])
        return [tok for par in par_toks for sent in par for tok in sent]
    




    