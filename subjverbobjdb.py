

import doctable


class SubjVerbObjDB(doctable.DocTable):
    tabname = 'subjverb'
    schema = (
        ('idcol', 'id'),
        
        # relations
        ('string', 'subject'),
        ('string', 'verb'),
        ('string', 'object'),
        #('integer', 'count'),
        
        # other metadata
        ('string', 'language'),
        ('integer', 'docid'),
        ('integer', 'gutenid'),
        
        # indices
        ('index', 'ind_subj', ['subject']),
        ('index', 'ind_verb', ['verb']),
        ('index', 'ind_obj', ['object']),
        ('index', 'ind_lang', ['language']),
        ('index', 'ind_triplet', ['subject', 'verb', 'object', 'gutenid'], dict(unique=True)),
    )
    def __init__(self, fname=None, timeout=300, **kwargs):
        
        super().__init__(fname=fname, 
                         schema=self.schema, 
                         tabname=self.tabname, 
                         connect_args={'timeout': timeout},
                         **kwargs
                        )


    
    