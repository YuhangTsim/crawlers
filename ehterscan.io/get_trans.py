"""
Get first two pages from https://etherscan.io
"""
import re
import requests

if __name__ == "__main__":
    result = []
    result.append(['tx_hash', 'tx_block', 'tx_from',
                   'tx_to', 'tx_time', 'tx_value'])
    for i in [1, 2]:
        # when ur click to the second page, the url is changed with parameters: ps stands for numberof record per page and p stands for page number.
        baseurl = 'https://etherscan.io/txs?ps=100&p={}'
        # you can find the page structure from browser in which each transaction is within an element enclosed by <tr> and </tr>, but the first line are table headers.
        resp = requests.get(baseurl.format(i))
        trans = re.findall('<tr>(.*)</tr>', resp.text)[0].split(u'</tr>')[1:]
        for tran in trans:
            if tran:
                tx = {}
                tds = tran.split('</td>')
                tx['tx_hash'] = re.findall(u'>(\S+)</a', tds[0])[0]
                tx['tx_block'] = re.findall(
                    u'href=\\\'/block/(\S+)?\'>', tran)[0]
                addresses = re.findall(u'href=\\\'/address/(\S+)?\'>', tran)
                tx['tx_from'] = addresses[0]
                tx['tx_to'] = addresses[1]
                tx['tx_time'] = re.findall(u'title=\\\'(.+)?\'>', tds[2])[0]
                if '</b>' in tds[6]:
                    tx['tx_value'] = re.findall('</b>(.*)', tds[6])[0]
                else:
                    tx['tx_value'] = re.findall(u'<td>(.*)', tds[6])[0]
                result.append(
                    [tx['tx_hash'],
                     tx['tx_block'],
                     tx['tx_from'],
                     tx['tx_to'],
                     tx['tx_time'],
                     tx['tx_value']])

    with open('./first_2_page_txs.csv', 'w') as f:
        print(len(result), 'records are crawled')
        for line in result:
            f.write(','.join(line)+'\n')
