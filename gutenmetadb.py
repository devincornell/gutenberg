import os
import sys
sys.path.append('../..')
import doctable
#import ..doctable

class GutenMetaTable(doctable.DocTable):
    '''DocTable used to store metadata. Will be used as member in GutenDocDB'''
    tabname = 'gutenmeta'
    def __init__(self, fname, **kwargs):
        self.schema = (
            ('idcol', 'id'),
            #('string', 'fname', dict(nullable=False)),
            ('integer', 'gutenid'),
            ('string', 'title'),
            ('string', 'author'),
            ('string', 'formaturi'),
            ('string', 'language'),
            ('string', 'rights'),
            ('string', 'subject'),
            
            # indices
            ('index', 'ind_gutenid', ['gutenid'], dict(unique=True)),
            ('index', 'ind_title', ['title'], dict(unique=True)),
        )
        doctable.DocTable.__init__(self, fname=fname, schema=self.schema, 
            tabname=self.tabname, **kwargs)
