from bs4 import BeautifulSoup


class HTMLParser(object):
    def __init__(self, html_source):
        self.soup = BeautifulSoup(html_source, 'html.parser')
        self.outputlist = []

    def handlestarttag(self):
        for link in self.soup.find_all("a"):
            self.outputlist.append(link.get('href'))
