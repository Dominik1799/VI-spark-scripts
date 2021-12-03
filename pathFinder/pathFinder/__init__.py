from queue import Queue
from typing import TextIO
import json


class PathFinder:
    # start_article -> article name, key in disk_map
    # end_artivle -> article name, key in disk_map
    # disk_map -> dictionary
    # database -> file pointer
    def __init__(self, start_article: str, end_article: str, disk_map: dict, parsed_articles: TextIO):
        self.__start_article = start_article
        self.__end_article = end_article
        self.__disk_map = disk_map
        self.__parsed_articles = parsed_articles
        self.__path = {}

    def find_path(self):
        queue = Queue()
        visited = set()

        queue.put(self.__start_article)
        visited.add(self.__start_article)

        while not queue.empty():
            current_article = queue.get()
            if current_article not in self.__disk_map:
                continue
            disk_offset = self.__disk_map[current_article]
            self.__parsed_articles.seek(disk_offset)
            record = json.loads(self.__parsed_articles.readline())
            # link == neighbour
            for link in record["links"]:
                if link in visited:
                    continue
                queue.put(link)
                visited.add(link)
                self.__path[link] = current_article

                if link == self.__end_article:
                    return True

        return False

    def print_path(self):
        articles = []
        current_article = self.__end_article
        articles.append(current_article)
        while True:
            articles.append(self.__path[current_article])
            if self.__path[current_article] == self.__start_article:
                break
            current_article = self.__path[current_article]

        for i in range(len(articles) - 1, -1, -1):
            print(articles[i])
