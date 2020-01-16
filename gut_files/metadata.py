from gutenberg.acquire import set_metadata_cache
from gutenberg.acquire import get_metadata_cache
from gutenberg.acquire.metadata import SqliteMetadataCache


if __name__ == '__main__':
    #cache = SqliteMetadataCache('metadata2.sqlite')
    #cache.populate()
    #set_metadata_cache(cache)
    
    
    cache = get_metadata_cache()
    cache.populate()
