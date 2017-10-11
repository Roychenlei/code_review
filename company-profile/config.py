import os


# project root dir
PROJ_DIR = os.path.dirname(os.path.abspath(__file__))


CACHE_ROOT = os.getenv('CACHE_ROOT', '/mnt/data/cache')


MONGO_HOST = os.getenv('MONGO_HOST', 'datascience1.dev.zippia.com')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'zippia_186_2')


WIKI_COLLECTION_PARSED = os.getenv('WIKI_COLLECTION_PARSED','wikipedia_parsed')

WIKI_COLLECTION_CLEAN = os.getenv('WIKI_COLLECTION_CLEAN','wikipedia_clean') 

WIKI_COLLECTION_ORIGINAL = os.getenv('WIKI_COLLECTION_ORIGINAL','wiki_collection_original')

WIKI_COLLECTION_INPUT = os.getenv('WIKI_COLLECTION_INPUT','filtered_company')

CRITERIA = {'Headquarters','Founded','Industry'}