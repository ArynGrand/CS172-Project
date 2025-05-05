import praw
import os
from dotenv import load_dotenv
import json
from multiprocessing import Manager, Process, Value
import requests
from bs4 import BeautifulSoup
import time
import re #package for reg ex expressions

# root needs a .env file with:
# CLIENT_ID, CLIENT_SECRET, USER_AGENT, USER_ID, USER_PASS
# eg: 
# CLIENT_ID="some_id"
# CLIENT_SECRET="some_secret_key"...
load_dotenv()

# Crawler class handles each Spider thread
# Each Spider has its own memory space, with some shared data
# Spiders can send data to each other through a shared list of queues
# Each Spider has its own queue that it reads from using its thread_id
# To determine which Spider is in charge of a specific reddit post we
# hash the subreddit name and send the **url** to the corresponding queue
#                                      ^^^^^^^
class Crawler:
    def __init__(self, seed_file, num_pages, size_limit, output_dir, 
                 num_procs, timeout, debug=False):
        # Each process has its own memory space
        # all self.* variables are NOT shared data
        # shared data is explicitly created
        
        start_time = time.time()
        start_size = size_limit

        if num_pages is None:
            num_pages = 9_223_372_036_854_775_807

        self.seed_file  = seed_file # file with reddit urls to start from
        # num_pages is shared data. The total number of pages to be processed
        self.num_pages  = Value('q', num_pages)
        self.size_limit = Value('q', size_limit) # total amount of data to store
        self.output_dir = output_dir # directory to save files
        self.num_procs  = num_procs # number of processes to spawn
        self.timeout    = timeout # timeout in seconds for each proc
        self.debug      = debug # debug mode
        
        # queues are shared data
        # each process can send data to the other processes through its queue
        # the manager simply creates a shared memory region to place the queues
        manager         = Manager()
        self.queues     = [manager.Queue() for _ in range(num_procs)]
        self.reddit     = self.get_reddit() # PRAW API
        self.sleep_time = 5 # time to sleep if the queue is empty
        self.load_seeds()

        self.visited    = set()

        # Each proc runs the spider method with its thread_id(i)
        procs = [Process(target=self.spider, args=(i,))
                 for i in range(num_procs)]

        # start all the processes
        for proc in procs:
            proc.start()

        # wait for all the processes to finish
        for proc in procs:
            proc.join()

        if self.num_pages.value <= 0:
            print("All pages processed.")
        elif self.size_limit.value <= 0:
            print("Size limit reached.")
        else:
            print("All threads done")

        sz = start_size - self.size_limit.value
        print(f"Processed {sz} bytes in {time.time() - start_time:.2f} seconds")

    # Create a PRAW Reddit instance using credentials from .env file
    def get_reddit(self): 
        reddit = praw.Reddit(
            client_id=os.getenv("CLIENT_ID"),
            client_secret=os.getenv("CLIENT_SECRET"),
            user_agent=os.getenv("USER_AGENT"),
            username=os.getenv("USER_ID"),
            password=os.getenv("USER_PASS"),
        )
        print("Authenticated as:", reddit.user.me())

        if not reddit:
            print("Reddit instance not created. Check your credentials.")
            exit(1)
        elif self.debug:
            print("Reddit instance created.")
        return reddit

    # Load the seed urls from the seed file and assign them to Spiders
    def load_seeds(self):
        if not os.path.exists(self.seed_file):
            print(f"Seed file {self.seed_file} does not exist.")
            exit(1)
        with open(self.seed_file, "r") as f:
            seeds = f.readlines()
        seeds = [seed.strip() for seed in seeds]
        for seed in seeds:
            self.queues[self.hash(seed)].put(seed)
        print(f"Loaded {len(seeds)} seeds from {self.seed_file}.")

    # Each Spider runs this method
    def spider(self, thread_id):
        self.thread_id = thread_id
        # ^^ note that self.thread_id can be used to identify
        # the thread from any other method
        
        print(f"Thread {thread_id} started.")
        # While there are still pages to process
        # no lock is aquired to just read the value
        while self.num_pages.value > 0 and self.size_limit.value > 0:
            try:
                # Queue.get() will raise an exception if the queue is empty
                # block=False means that the thread will not stall waiting
                # for a new item to be inserted
                url = self.queues[self.thread_id].get(block=False)
            except Exception as e:
                # If the queue is empty, wait a bit and try again
                # if we wait too much the thread will *unalive* itself
                self.timeout -= self.sleep_time
                if self.timeout < 0:
                    print(f"Thread {self.thread_id} timed out.")
                    return
                if self.debug:
                    print(f"Thread {self.thread_id} waiting...")
                time.sleep(self.sleep_time)
                continue

            # If we got a url do some work with it
            self.parse_url(url)

    # Parse a given url, make a submission object to start parsing data
    def parse_url(self, url):
        if url in self.visited:
            if self.debug:
                print(f"Thread {self.thread_id} already visited {url}")
            return
        self.visited.add(url)

        # only try again if we get a 429 error for too many requests
        while True:
            try:
                if self.debug:
                    print(f"Thread {self.thread_id} processesing URL: {url}")
           
                self.parse_submission(self.reddit.submission(url=url))

                # if we successfully got the submission and data,
                # count this page as processed
                with self.num_pages.get_lock():
                    self.num_pages.value -= 1
            
                break
        
            except Exception as e:
                if str(e) == "received 429 HTTP response":
                    print(f"Thread {self.thread_id} rate limited. Sleeping...")
                    time.sleep(2)
                    continue
                print(f"Thread {self.thread_id} error processing {url}: {e}")
                break

    # Given some text extract all the subreddits in the form r/subreddit_name
    def extract_subreddits(self, text: str) -> list[str]:
        pattern = re.compile(r'r/([A-Za-z0-9_]+)')
        # findall returns a list of capture‐group strings
        return pattern.findall(text)
    
    # Given some text extract all the urls in the form (http://*) or (https://*)
    def extract_urls(self, text: str) -> list[str]:
        URL_REGEX = re.compile(
            r'(https?://[^\s]+)|(www\.[^\s]+)',
            flags=re.IGNORECASE
        )
        return URL_REGEX.findall(text)
    
    # Hash a value to determine which queue to send it to
    def hash(self, value):
        return abs(hash(value)) % self.num_procs

    # Parse reddit comment, extract subreddits and links
    def parse_comment(self, comment, comments_out, links_out):
        subreddits = self.extract_subreddits(comment.body)
        links = self.extract_urls(comment.body)
        for subreddit in subreddits:
            subreddit = self.reddit.subreddit(subreddit)
            posts = subreddit.new(limit=1000)
            for post in posts:
                self.queues[self.hash(post.url)].put(post.url)
        for link in links:
            try:
                # if the link is not a reddit post
                # this will raise an error
                submission = self.reddit.submission(url=link)
                subreddit = submission.subreddit.display_name
                self.queues[self.hash(subreddit)].put(submission.url)
            except Exception as e:
                links_out.append(link)

        comments_out.append(comment.body)

    # parse a reddit post and save it to a file, saves:
    # ID, Subreddit, Author, Timestamp, Title, Post text, URL,
    # Comments, and links to non reddit.com websites
    def parse_submission(self, submission):
        comments = []
        external_links = []
        if self.debug:
            print(f"Thread {self.thread_id} parsing comments")
        submission.comments.replace_more(limit=None)
        for comment in submission.comments.list():
            self.parse_comment(comment, comments, external_links)
        if self.debug:
            print(f"Thread {self.thread_id} finished parsing comments")

        postData = { #download data and info
            "id":             submission.id,
            "subreddit":      submission.subreddit.display_name,
            "author":         str(submission.author),
            "created_utc":    submission.created_utc,
            "title":          submission.title,
            "selftext":       submission.selftext,
            "url":            submission.url,
            "comments":       comments,
            "external_links": external_links 
        }
        self.save_to_json(postData) #call saving function

    def get_html_title(self, url):
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=5)
            if response.ok:
                soup = BeautifulSoup(response.text, 'html.parser')
                return soup.title.string.strip() if soup.title else None
        except Exception as e:
            if self.debug:
                print(f"Error fetching title for {url}: {e}")
        return None

    def save_to_json(self, post_dict):
        if self.debug:
            print(f"Thread {self.thread_id} saving data to JSON")
        os.makedirs(self.output_dir, exist_ok=True)
        file_prefix = os.path.join(self.output_dir, "reddit_data")
        file_index = 0
        while os.path.exists(f"{file_prefix}_{file_index}.json") and os.path.getsize(f"{file_prefix}_{file_index}.json") >= 10*1024*1024:
            file_index += 1
        with open(f"{file_prefix}_{file_index}.json", 'a', encoding='utf-8') as f:
            json.dump(post_dict, f)
            f.write("\n")

        # size of this json object in bytes
        # removes the size from the size limit
        jsz = len(json.dumps(post_dict).encode('utf-8'))
        with self.size_limit.get_lock():
            self.size_limit.value -= jsz

        print(f"Data remaining: {self.size_limit.value} bytes")

if __name__ == "__main__":
    crawler = Crawler(
                seed_file="seeds.txt",
                num_pages=None,
                size_limit=500*1024*1024, # 500 MB
                output_dir="output",
                num_procs=16,
                timeout=30*60, # 30 minutes
                debug=True
            )
