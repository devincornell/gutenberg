#from gutenberg.acquire import get_metadata_cache

from gutenberg.acquire.metadata import SleepycatMetadataCache

#from gutenberg.acquire import set_metadata_cache
#from gutenberg.acquire.metadata import SqliteMetadataCache
#cache = SqliteMetadataCache('/my/custom/location/cache.sqlite')
#cache.populate()
#set_metadata_cache(cache)

if __name__ == '__main__':
    #cache = get_metadata_cache()local/code/doctable/examples/gutenberg
    cache = SleepycatMetadataCache('guten_cache.sqlite')
    cache.populate()
    set_metadata_cache(cache)
