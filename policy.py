import abc
import os
from urllib.parse import urlparse

import numpy as np


class Policy(abc.ABC):

    @abc.abstractmethod
    def getURL(self, c, _):
        pass

    @abc.abstractmethod
    def updateURLs(self, c, retrievedURLs, retrievedURLsWD, iteration):
        pass

    @abc.abstractmethod
    def reset(self):
        pass


class DummyPolicy(Policy):
    def getURL(self, c, _):
        if len(c.URLs) == 0:
            return None
        else:
            return c.seedURLs[0]

    def updateURLs(self, c, retrievedURLs, retrievedURLsWD, iteration):
        pass

    def reset(self):
        pass


class LIFOPolicy(Policy):
    queue = None

    def getURL(self, c, _):
        if self.queue is None:
            self.queue = c.seedURLs.copy()
        if not self.queue:
            return None
        return self.queue.pop()

    def updateURLs(self, c, retrievedURLs, *unused):
        def extract_filename(url):
            return os.path.basename(urlparse(url).path)

        retrievedURLs = sorted(retrievedURLs, key=lambda url: extract_filename(url))
        self.queue += retrievedURLs

    def reset(self):
        pass


class FIFOPolicy(Policy):
    queue = None

    def getURL(self, c, _):
        if self.queue is None:
            self.queue = c.seedURLs.copy()
        if not self.queue:
            return None
        return self.queue.pop(0)

    def updateURLs(self, _, retrievedURLs, *unused):
        def extract_filename(url):
            return os.path.basename(urlparse(url).path)

        retrievedURLs = sorted(retrievedURLs, key=lambda url: extract_filename(url))
        self.queue += retrievedURLs

    def reset(self):
        pass


class LIFO_Cycle_Policy(Policy):
    queue = None
    fetched = set()

    def getURL(self, c, _):
        if self.queue is None:
            self.queue = c.seedURLs.copy()
        if not self.queue:
            return None
        next_url = self.queue.pop()
        while next_url in self.fetched:
            if not self.queue:  # empty list
                return None
            next_url = self.queue.pop()
        self.fetched.add(next_url)
        return next_url

    def updateURLs(self, c, retrievedURLs, *unused):
        def extract_filename(url):
            return os.path.basename(urlparse(url).path)

        retrievedURLs = sorted(retrievedURLs, key=lambda url: extract_filename(url))
        self.queue += retrievedURLs

    def reset(self):
        self.fetched = set()


class LIFOAuthorityPolicy(Policy):
    queue = None
    fetched = set()

    def getURL(self, c, _):
        if self.queue is None:
            self.queue = c.seedURLs.copy()
        if not self.queue:
            return None

        def choose_next():
            if not c.authority:
                return self.queue.pop(0)
            prob = np.array(list(c.authority.values()))
            prob = prob / np.sum(prob)  # normalize
            result_url = np.random.choice(list(c.authority.keys()), p=prob)
            return result_url

        next_url = choose_next()
        if not c.authority:
            while next_url in self.fetched:
                if not self.queue:  # empty list
                    return None
                next_url = choose_next()
            self.fetched.add(next_url)
        return next_url

    def updateURLs(self, c, retrievedURLs, *unused):
        def extract_filename(url):
            return os.path.basename(urlparse(url).path)

        retrievedURLs = sorted(retrievedURLs, key=lambda url: extract_filename(url))
        self.queue += retrievedURLs

    def reset(self):
        self.fetched = set()
