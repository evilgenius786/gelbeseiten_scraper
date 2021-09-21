import csv
import json
import os
import re
import threading
import time
import traceback

import pandas as pd
import requests
from bs4 import BeautifulSoup

threadcount = 10

out_file = 'out.csv'
header = ['Branch', 'Name', 'Website', 'Blank', 'Name', 'Street', 'ZIP', 'City', 'Phone', 'Blank', 'Email', 'Website','URL']
z_file = 'zip.csv'
semaphore = threading.Semaphore()
write = threading.Lock()


def process(url):
    with semaphore:
        try:
            soup = BeautifulSoup(requests.get(url).content, 'lxml')
            script = soup.find('script', {'type': "application/ld+json"})
            js = json.loads(script.contents[0])['@graph'][0]
            # addr = soup.find('address', {"typeof": "PostalAddress"}).find_all('span')
            # data = {
            #     'Branch': [x.strip() for x in
            #                soup.find('div', {'class': 'mod-TeilnehmerKopf__branchen'}).text.split('\n')[2:]
            #                if x.strip()],
            #     'Name': soup.find('h1', {'class': "mod-TeilnehmerKopf__name"}).text.strip(),
            #     'Website': get(soup, 'li', {'class', 'mod-Kontaktdaten__list-item contains-icon-homepage'}),
            #     'Email': get(soup, 'li', {'class', 'mod-Kontaktdaten__list-item contains-icon-email'}),
            #     'Street': addr[0].text,
            #     'ZIP': addr[1].text,
            #     'City': addr[2].text,
            #     'Phone': soup.find('span', {'class': 'mod-TeilnehmerKopf--secret_suffix'}).text.strip(),
            #     'URL': url
            # }
            data = {
                'Branch': ", ".join([x.strip() for x in
                                     soup.find('div', {'class': 'mod-TeilnehmerKopf__branchen'}).text.split('\n')[2:]
                                     if x.strip()]),
                'Name': js['name'],
                'Website': js['url'] if 'url' in js.keys() else "",
                'Email': js['email'] if 'email' in js.keys() else "",
                'Street': js['address']['streetAddress'],
                'ZIP': js['address']['postalCode'],
                'City': js['address']['addressLocality'],
                'Phone': f"0049{js['telephone'][1:]}" if 'telephone' in js.keys() else "",
                'URL': url
            }
            print(json.dumps(data, indent=4))
            # data.pop('URL')
            threading.Thread(target=append,args=(data,)).start()
            # append(data)
        except:
            print("Error on", url)
            traceback.print_exc()
            append_error(url)


def convert():
    while True:
        time.sleep(10)
        with write:
            print("Converting...")
            df = pd.read_csv(out_file)
            df.to_excel(out_file.replace('csv', 'xlsx'), index=None, header=True)


def main():
    os.system('color 0a')
    logo()
    url = 'https://www.gelbeseiten.de/Suche/Wintergärten/'
    if not os.path.isfile(out_file):
        with open(out_file, 'w', encoding='utf8', newline='') as ofile:
            csv.DictWriter(ofile, fieldnames=header).writeheader()
    zip_list = []
    with open(z_file, encoding='utf8') as zipfile:
        for line in csv.reader(zipfile):
            zip_list.append(re.findall('\d+', line[0].strip())[0])
    out = []
    with open(out_file, encoding='utf8') as ofile:
        for row in csv.reader(ofile):
            out.append(row)
    # zip_list = ['48341']
    threads = []
    if os.path.isfile('error.txt'):
        print('Working on errorfile.')
        with open('error.txt') as efile:
            hrefs = efile.read().splitlines()
        for href in hrefs:
            if href not in str(out):
                print(href)
                # process(href)
                thread = threading.Thread(target=process, args=(href,))
                thread.start()
                threads.append(thread)
            else:
                print("Already scraped", href)
        with open('error.txt', 'w') as efile:
            efile.write("")
        for t in threads:
            t.join()
        print("Done with error file")
    threads = []
    threading.Thread(target=convert).start()
    for z_code in zip_list:
        print(url + z_code)
        soup = BeautifulSoup(requests.get(url + z_code).text, 'lxml')
        # for article in soup.find('div', {'class': "mod-TrefferListe"}).find_all('article'):
        for article in soup.find('div', {'id': "gs_treffer"}).find_all('article'):
            try:
                if article.find('a') is not None:
                    href = article.find('a')['href']
                    if article.find('address') is not None and z_code in article.find('address').text:
                        if href not in str(out):
                            print(z_code, href)
                            # process(href)
                            thread = threading.Thread(target=process, args=(href,))
                            thread.start()
                            threads.append(thread)
                        else:
                            print("Already scraped", href)
            except:
                print(article)
                traceback.print_exc()
        #     break
        # break
    for t in threads:
        t.join()


def append_error(row):
    with write:
        with open('error.txt', 'a') as efile:
            efile.write(row + '\n')


def append(row):
    with write:
        with open(out_file, 'a', encoding='utf8', newline='') as ofile:
            csv.DictWriter(ofile, fieldnames=header).writerow(row)


def get(soup, tag, attrib):
    try:
        return [x.text.strip() for x in soup.find_all(tag, attrib)]
    except:
        return ""


def logo():
    print(f"""
      ________       .__ ___.              _________      .__  __                 
     /  _____/  ____ |  |\_ |__   ____    /   _____/ ____ |__|/  |_  ____   ____  
    /   \  ____/ __ \|  | | __ \_/ __ \   \_____  \_/ __ \|  \   __\/ __ \ /    \ 
    \    \_\  \  ___/|  |_| \_\ \  ___/   /        \  ___/|  ||  | \  ___/|   |  \\
     \______  /\___  >____/___  /\___  > /_______  /\___  >__||__|  \___  >___|  /
            \/     \/         \/     \/          \/     \/              \/     \/ 
=========================================================================================
                    GelbeSeiten.de business details scraper by: 
                            fiverr.com/muhammadhassan7
=========================================================================================
[+] Multithreaded ({threadcount})
[+] Resumable
[+] Duplicate remover
_________________________________________________________________________________________
""")


if __name__ == '__main__':
    main()
