import pandas as pd
import requests

import urllib
import json
from tqdm import tqdm_notebook


def GetWatchDetails(name, price, id):
    """Get listing details for any watch given a id.htm link"""
    if id in pd.read_feather('SavedSeaches.feather')['index'].values:
        return pd.read_feather('ListingDB.feather').set_index('index').loc[id]
    url = f'https://www.chrono24.com.au/rolex/{id}'
    try:
        r = requests.get(url)
        page = pd.read_html(r.text)
    except Exception as e:
        return(e, url)
    tempdf = pd.concat([page[0].set_index(0).transpose(),page[1].set_index(0).transpose()], axis=1)
    if 'Others' in tempdf.columns:
        tempdf = tempdf.loc[:,:'Others']
    if 'Functions' in tempdf.columns:
        tempdf = tempdf.loc[:,:'Functions']
    tempdf['price'] = price
    tempdf['name'] = name
    tempdf['url'] = url
    tempdf['retrieved'] = pd.Timestamp('now')
    tempdf.index = [id]
    tempdf = tempdf.groupby(level=0, axis=1).last()
    if not tempdf.empty:
        savedf = pd.read_feather('ListingDB.feather').set_index('index')
        savedf = savedf.append(tempdf)
        savedf.reset_index()[['index']].to_feather('SavedSeaches.feather')
        savedf.reset_index().to_feather('ListingDB.feather')
    return(tempdf)

class ScrapeError(Exception):
    """Catch any errors with scraping data"""
    pass

def GetListings(filters={}, brand='search', pages=1):
    """Get listings for a given brand and filters"""
    params = {'showpage':1,'pageSize':120,'sortorder':5,'resultview':'block', 'dosearch':True}
    # sortorder:5 = newest first
    if filters:
        params.update(filters)
    outdf = pd.DataFrame()
    for index in tqdm_notebook(range(pages)):
        params.update({'showpage':index})
        url = f'https://www.chrono24.com.au/{brand}/index.htm?{urllib.parse.urlencode(params)}'
        r = requests.get(url)
        if r.url != url:
            raise ScrapeError('Request blocked!')
            return
        j = r.text.split('<script type="application/ld+json">\n')[1].split('\n   </script>')[0].replace('\n','').replace('\t','')
        j = json.loads(j) 
        df = pd.json_normalize(j['@graph'][1]['offers']) 
        outdf = outdf.append(df, sort=False)
    outdf['price'] = pd.to_numeric(outdf['price'])
    return(outdf)