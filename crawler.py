import praw
import os
from dotenv import load_dotenv
import json
from multiprocessing import Manager, Process, Value
import time

# root needs a .env file with:
# CLIENT_ID, CLIENT_SECRET, USER_AGENT, USER_ID, USER_PASS
# eg: 
# CLIENT_ID="some_id"
# CLIENT_SECRET="some_secret_key"...
load_dotenv()

# Exception raised for errors in the parsing process.
class ParseError(Exception): 
    pass

# Crawler class handles each Spider thread
# Each Spider has its own memory space, with some shared data
# Spiders can send data to each other through a shared list of queues
# Each Spider has its own queue that it reads from using its thread_id
# To determine which Spider is in charge of a specific reddit post we
# hash the subreddit name and send the **url** to the corresponding queue
#                                      ^^^^^^^
class Crawler:
    def __init__(self, seed_file, num_pages, hops_away, output_dir, 
                 num_procs, timeout, debug=False):
        # Each process has its own memory space
        # all self.* variables are NOT shared data
        # shared data is explicitly created
        
        self.seed_file  = seed_file # file with reddit urls to start from
        # num_pages is shared data. The total number of pages to be processed
        self.num_pages  = Value('i', num_pages)
        self.hops_away  = hops_away # max hops from the seed file
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
        self.sleep_time = 0.5 # time to sleep if the queue is empty

        self.load_seeds()

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
        else:
            print("All threads done")

    # Create a PRAW Reddit instance using credentials from .env file
    def get_reddit(self):
        reddit = praw.Reddit(
            client_id=os.getenv("CLIENT_ID"),
            client_secret=os.getenv("CLIENT_SECRET"),
            user_agent=os.getenv("USER_AGENT"),
            username=os.getenv("USER_ID"),
            password=os.getenv("USER_PASS"),
        )
        if not reddit:
            print("Reddit instance not created. Check your credentials.")
            exit(1)
        elif self.debug:
            print("Reddit instance created.")
        return reddit

    # Load the seed urls from the seed file and assign them to Spiders
    # right now simply puts them in each queue in order
    # TODO: each thread should be in charge of a specific subreddit,
    # hash the url and send it to the correct queue
    def load_seeds(self):
        if not os.path.exists(self.seed_file):
            print(f"Seed file {self.seed_file} does not exist.")
            exit(1)
        with open(self.seed_file, "r") as f:
            seeds = f.readlines()
        seeds = [seed.strip() for seed in seeds]
        for i, seed in enumerate(seeds):
            self.queues[i%self.num_procs].put(seed)
        print(f"Loaded {len(seeds)} seeds from {self.seed_file}.")

    # Each Spider runs this method
    def spider(self, thread_id):
        self.thread_id = thread_id
        # ^^ note that self.thread_id can be used to identify
        # the thread from any other method
        
        print(f"Thread {thread_id} started.")
        # While there are still pages to process
        # no lock is aquired to just read the value
        while self.num_pages.value > 0:
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
        # parse all the links in the post and comments, add them
        # to the queues and reduce hops_away, do not add any
        # links to the queue if hops_away == 0
        try:
            if self.debug:
                print(f"Thread {self.thread_id} processesing URL: {url}")
            
            self.parse_submission(self.reddit.submission(url=url))

            # if we successfully got the submission and data,
            # count this page as processed
            with self.num_pages.get_lock():
                self.num_pages.value -= 1
           
            # how to send data to other threads
            # if self.thread_id == 1:
            #     self.queues[0].put(url)
        
        except ParseError as e:
            print(f"Thread {self.thread_id} ParseError: {e}")
        except Exception as e:
            print(f"Thread {self.thread_id} error processing {url}: {e}")

    # TODO: Parse a reddit post:
    # gets all the data and saves it to a file
    # extracts all the links from the post and comments and gives them
    # to their corresponding queues
    # Raises ParseErrors if something goes wrong
    # If a comment or post has a link to a subreddit instead of a post,
    # do something like get the 100 newest posts, or 100 hottes posts, etc
    def parse_submission(self, submission):
        submission.comments.replace_more(limit=0)
        comments = [comment.body for comment in 
                    submission.comments.list()]
        
        # how to raise ParseError
        # raise ParseError("some parse_submission error")

        if self.debug:
            print(f"Thread {self.thread_id} found {len(comments)} comments.")

if __name__ == "__main__":
    crawler = Crawler("seeds.txt", 100, 2, "output", 2, 2, True)
