"""
CAPP 30122: Course Search Engine Part 1

Wanxi Zhou
"""
# DO NOT REMOVE THESE LINES OF CODE
# pylint: disable-msg=invalid-name, redefined-outer-name, unused-argument, unused-variable

import queue
import json
import sys
import csv
import re
import bs4
import util

INDEX_IGNORE = set(['a', 'also', 'an', 'and', 'are', 'as', 'at', 'be',
                    'but', 'by', 'course', 'for', 'from', 'how', 'i',
                    'ii', 'iii', 'in', 'include', 'is', 'not', 'of',
                    'on', 'or', 's', 'sequence', 'so', 'social', 'students',
                    'such', 'that', 'the', 'their', 'this', 'through', 'to',
                    'topics', 'units', 'we', 'were', 'which', 'will', 'with',
                    'yet'])


def construct_corpus(string):
    '''
    Construct a word set given a string, with elements in the INDEX_IGNORE
    set excluded.

    Inputs:
        string: a string

    Outputs:
        a words set
    '''
    rv = set()
    word_lst = string.lower().split()

    for word in word_lst:
        w = re.findall(r"^[a-z]+\w*", word)
        if (len(w) > 0) and (w[0] not in INDEX_IGNORE):
            rv.add(w[0])

    return rv
    

def indexer_helper(course, word_map, is_seq=False, desc_seq=None):
    '''
    A helper funtion for the indexer function

    Inputs:
        course: a "courseblock main" tag
        word_map: a dictionary mapping words to course codes
        is_seq: whether the input course is in a sequence
        desc_seq: the sequence description if is_seq is true
    '''
    title = course.find("p", class_="courseblocktitle").text
    code = re.sub(r"\xa0", " ", title.split(".")[0])
    desc = course.find("p", class_="courseblockdesc").text.strip()
    words = " ".join([title, desc])

    if is_seq:
        words = " ".join([words, desc_seq])

    word_set = construct_corpus(words)
    for word in word_set:
        if word not in word_map:
            word_map[word] = set()
        word_map[word].add(code)


def indexer(soup, word_map):
    '''
    Construct a dictionary mapping words to course codes.

    Inputs:
        soup: a Beautiful soup
        word_map: a dictionary mapping words to course codes
    '''
    course_lst = soup.find_all("div", class_="courseblock main")
    if len(course_lst) > 0:
        for course in course_lst:
            seq = util.find_sequence(course)
            if len(seq) > 0:
                desc = course.find("p", class_="courseblockdesc").text.strip()
                for c in seq:
                    indexer_helper(c, word_map, is_seq=True, desc_seq=desc)
            else:
                indexer_helper(course, word_map)


def queue_url(soup, queue, previous_url, limiting_domain, processed_url):
    '''
    Queue urls contained in the given soup.

    Inputs:
        soup: a Beautiful soup
        queue: a queue
        prevous_url: the url of the previous page
        processed_url: a set containing crawled urls
    '''
    s = set()
    for i in soup.find_all("a"):

        if i.has_attr("href"):
            url2 = util.remove_fragment(i["href"])
            url = util.convert_if_relative_url(previous_url, url2)

            if (url is not None) and (url not in s) and \
                (url not in processed_url) and \
                util.is_url_ok_to_follow(url, limiting_domain):
                queue.put(url)
                s.add(url)


def load_json(course_map_filename):
    '''
    Load json files.

    Inputs:
        course_map_filename: a JSON filename string

    Outputs:
        a json object mapping course codes to course indentifiers
    '''
    try:
        with open(course_map_filename) as f:
            course_map = json.load(f)

    except IOError:
        print("Could not read from file: {}".format(course_map_filename))
        return None

    return course_map


def write_index_csv(index_filename, course_map, word_map):
    '''
    Write a csv file.

    Inputs:
        index_filename: the output filename
        course_map: 
          a JSON object mapping course codes to course indentifiers
        word_map: a dictionary mapping words to course codes

    Outputs:
        a CSV file
    '''
    try:
        with open(index_filename, "w", newline="",
                  encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile, delimiter="|")
            for word, courses in word_map.items():
                for course in courses:
                    writer.writerow([course_map[course], word])

    except IOError:
        print("Could not write to file:{}".format(index_filename))
        return None

    return csvfile


def go(num_pages_to_crawl, course_map_filename, index_filename):
    '''
    Crawl the college catalog and generates a CSV file with an index.

    Inputs:
        num_pages_to_crawl: the number of pages to process during the crawl
        course_map_filename: the name of a JSON file that contains the mapping
          course codes to course identifiers
        index_filename: the name for the CSV of the index

    Outputs:
        CSV file of the index
    '''
    starting_url = ("http://www.classes.cs.uchicago.edu/archive/2015/winter"
                    "/12200-1/new.collegecatalog.uchicago.edu/index.html")
    limiting_domain = "classes.cs.uchicago.edu"
    word_map = {}
    course_map = load_json(course_map_filename)

    q = queue.Queue()
    processed_url = set()
    q.put(starting_url)

    while len(processed_url) < num_pages_to_crawl:
        if q.empty():
            break
        else:
            url1 = q.get()
            if url1 not in processed_url:
                processed_url.add(url1)
                r = util.get_request(url1)
                if r is not None:
                    text = util.read_request(r)
                    if len(text) > 0:
                        soup = bs4.BeautifulSoup(text, "html5lib")
                        indexer(soup, word_map)
                        queue_url(soup, q, url1, limiting_domain, processed_url)

    return write_index_csv(index_filename, course_map, word_map)


if __name__ == "__main__":
    usage = "python3 crawl.py <number of pages to crawl>"
    args_len = len(sys.argv)
    course_map_filename = "course_map.json"
    index_filename = "catalog_index.csv"
    if args_len == 1:
        num_pages_to_crawl = 1000
    elif args_len == 2:
        try:
            num_pages_to_crawl = int(sys.argv[1])
        except ValueError:
            print(usage)
            sys.exit(0)
    else:
        print(usage)
        sys.exit(0)

    go(num_pages_to_crawl, course_map_filename, index_filename)
