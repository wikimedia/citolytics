#!/bin/python

import requests

# src/main/resources/invalid_namespaces.txt
output_filename = __file__ + '.txt'

# Filter by different Wikipedia projects
# e.g. [wiki, wiktionary, wikibooks, wikinews, wikiquote, wikisource, wikiversity, wikivoyage]
filter_code = ['wiki']

# Collect languages
langs = []
sites = requests.get('https://commons.wikimedia.org/w/api.php?action=sitematrix&smtype=language&format=json').json()

for lang_id in sites['sitematrix']:
    lang = sites['sitematrix'][lang_id]
    if isinstance(lang, dict):
        for site in lang['site']:
            if site['code'] in filter_code:
                langs.append(lang['code'])
                break

# Collect invalid namespaces for each language
# langs = ['nds']
invalid_ns = []
#ns_str = ''
output_file = open(output_filename, 'w+')

for lang in langs:
    url = 'https://%s.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=namespaces&format=json' % lang
    res = requests.get(url).json()

    for ns in res['query']['namespaces']:
        if ns != '0':  # Invalid are all other name spaces
            # print(ns)
            ns_str = '%s:\n' % (res['query']['namespaces'][ns]['canonical']).lower().encode('utf-8')
            if ns_str not in invalid_ns:
                invalid_ns.append(ns_str)
                output_file.write(ns_str)

# Write to file
output_file.close()

# print(ns_str)
# print(invalid_ns)
# print(langs)
# print(sites)

