import praw
import os
from dotenv import load_dotenv
import json
from multiprocessing import Manager, Process, Value
import time

# root needs a .env file with:
# CLIENT_ID, CLIENT_SECRET, USER_AGENT, USER_ID, USER_PASS
load_dotenv()

class ParseError(Exception):
    """Exception raised for errors in the parsing process."""
    pass

class Crawler:
    def __init__(self, seed_file, num_pages, hops_away, output_dir, 
                 num_procs, timeout, debug=False):
        # Each process has its own memory space
        # all self.* variables are NOT shared data
        # shared data is explicitly created
        self.seed_file  = seed_file
        # num_pages is shared data. The total number of pages to be processed
        self.num_pages  = Value('i', num_pages)
        self.hops_away  = hops_away
        self.output_dir = output_dir
        self.num_procs  = num_procs
        self.timeout    = timeout
        self.debug      = debug
        
        # queues are shared data
        # each process can send data to the other processes through its queue
        manager         = Manager()
        self.queues     = [manager.Queue() for _ in range(num_procs)]
        self.reddit     = self.get_reddit()
        self.sleep_time = 0.5

        self.load_seeds()

        procs = [Process(target=self.spider, args=(i,)) 
                 for i in range(num_procs)]

        for proc in procs:
            proc.start()

        for proc in procs:
            proc.join()

        if self.num_pages.value <= 0:
            print("All pages processed.")
        else:
            print("All threads done")
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

    # load seeds from a file of reddit urls
    # right now simply puts them in each queue in order
    # each thread should be in charge of a subreddit in the future
    # make some hash function to do this
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

    def spider(self, thread_id):
        self.thread_id = thread_id
        print(f"Thread {thread_id} started.")
        while self.num_pages.value > 0:
            try:
                url = self.queues[self.thread_id].get(block=False)
            except Exception as e:
                self.timeout -= self.sleep_time
                if self.timeout < 0:
                    print(f"Thread {self.thread_id} timed out.")
                    return
                if self.debug:
                    print(f"Thread {self.thread_id} waiting...")
                time.sleep(self.sleep_time)
                continue

            self.parse_url(url)

    def parse_url(self, url):  
        # parse all the links in the post and comments, add them
        # to the queues and reduce hops_away, do not add any
        # links to the queue if hops_away == 0
        try:
            # count this page
            with self.num_pages.get_lock():
                self.num_pages.value -= 1

            if self.debug:
                print(f"Thread {self.thread_id} processesing URL: {url}")
            
            self.parse_submission(self.reddit.submission(url=url))
           
            # how to send data to other threads
            # if self.thread_id == 1:
            #     self.queues[0].put(url)

            # if something goes wrong processing the url raise an error 
            # raise ValueError("parse_url error")
        
        except ParseError as e:
            # dont count this page if we couldnt parse it
            with self.num_pages.get_lock():
                self.num_pages.value += 1
            print(f"Thread {self.thread_id} ParseError: {e}")
        except Exception as e:
            print(f"Thread {self.thread_id} error: {e}")

    # Raises ParseErrors if something goes wrong
    # temp do stuff with the post
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
