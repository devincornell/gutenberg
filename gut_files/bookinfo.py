

from gutenberg.query import get_etexts
from gutenberg.query import get_metadata
from gutenberg.acquire import load_etext
from gutenberg.cleanup import strip_headers
from gutenberg.query import list_supported_metadatas

from gutenberg.acquire import set_metadata_cache
from gutenberg.acquire.metadata import SqliteMetadataCache

if __name__ == '__main__':
    
    # load cache from db
    #cache = SqliteMetadataCache('metadata2.sqlite')
    #set_metadata_cache(cache)
    #print(cache.is_open)
    #cache.open()
    #md_attrs = list_supported_metadatas()
    
    features = ["author", "formaturi", "language", "rights", "subject", "title",]
    last_ebook_id = 61041
    i = 1
    while i <= last_ebook_id:
        if i % 100 == 0:
            print(f'on {i}')
        for feature_name in features:
            data = get_metadata(feature_name, i)
            print(feature_name, data)
        text = strip_headers(load_etext(i)).strip().replace('\r','')
        print(text[:10])
        print('\n\n')
        
    
    
    
    
    
    #print(text)  # prints 'MOBY DICK; OR THE WHALE\n\nBy Herman Melville ...'
    #print(type(cache))
    #print(cache.is_open)
    #print(len(cache.graph))
    #for s,p,o in cache.graph:
    #    print(s,p,o)
    #    break
    
    
    
    