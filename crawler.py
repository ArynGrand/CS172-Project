import praw
import os
from dotenv import load_dotenv
import json
from multiprocessing import Manager, Process, Value
import requests
from bs4 import BeautifulSoup
import time
import re
import argparse

load_dotenv()

class Crawler:
    def __init__(self, seed_file, num_pages, size_limit, output_dir,
                 num_procs, debug=False, hops_away=1, timeout=60):
        start_time = time.time()
        start_size = size_limit

        if num_pages is None:
            num_pages = 9_223_372_036_854_775_807

        self.seed_file  = seed_file
        self.num_pages  = Value('q', num_pages)
        self.size_limit = Value('q', size_limit)
        self.output_dir = output_dir
        self.num_procs  = num_procs
        self.debug      = debug
        self.hops_away  = hops_away
        self.timeout    = timeout

        manager         = Manager()
        self.queues     = [manager.Queue() for _ in range(num_procs)]
        self.reddit     = self.get_reddit()
        self.load_seeds()
        self.visited    = set()
        self.stop_all_threads = Value("b", 0)

        procs = [Process(target=self.spider, args=(i,)) for i in range(num_procs)]
        for proc in procs:
            proc.start()

        while self.stop_all_threads.value == 0:
            time.sleep(15)
            s = [x.qsize() for x in self.queues]
            print(f"Queues: {s}")
            if sum(s) == 0:
                self.stop_all_threads.value = 1

        for proc in procs:
            proc.terminate()

        if self.num_pages.value <= 0:
            print("All pages processed.")
        elif self.size_limit.value <= 0:
            print("Size limit reached.")
        else:
            print("All threads done")

        sz = start_size - self.size_limit.value
        print(f"Processed {sz} bytes in {time.time() - start_time:.2f} seconds")

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

    def spider(self, thread_id):
        self.thread_id = thread_id
        print(f"Thread {thread_id} started.")
        while self.num_pages.value > 0 and self.size_limit.value > 0:
            url = self.queues[self.thread_id].get()
            self.parse_url(url)
        self.stop_all_threads.value = 1

    def parse_url(self, url):
        if url in self.visited:
            print(f"Thread {self.thread_id} already visited {url}")
            return
        self.visited.add(url)

        while True:
            try:
                if self.debug:
                    print(f"Thread {self.thread_id} processesing URL: {url}")
                self.parse_submission(self.reddit.submission(url=url))
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

    def extract_subreddits(self, text: str) -> list[str]:
        pattern = re.compile(r'r/([A-Za-z0-9_]+)')
        return pattern.findall(text)

    def extract_urls(self, text: str) -> list[str]:
        URL_REGEX = re.compile(
            r'(https?://[^\s]+)|(www\.[^\s]+)',
            flags=re.IGNORECASE
        )
        return [u[0] if u[0] else u[1] for u in URL_REGEX.findall(text)]

    def hash(self, value):
        return abs(hash(value)) % self.num_procs

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
                submission = self.reddit.submission(url=link)
                subreddit = submission.subreddit.display_name
                self.queues[self.hash(subreddit)].put(submission.url)
            except Exception:
                links_out.append(link)
        comments_out.append(comment.body)

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

        postData = {
            "id": submission.id,
            "subreddit": submission.subreddit.display_name,
            "author": str(submission.author),
            "created_utc": submission.created_utc,
            "title": submission.title,
            "selftext": submission.selftext,
            "url": submission.url,
            "comments": comments,
            "external_links": external_links
        }
        self.save_to_json(postData)

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
        jsz = len(json.dumps(post_dict).encode('utf-8'))
        with self.size_limit.get_lock():
            self.size_limit.value -= jsz
        print(f"Data remaining: {self.size_limit.value} bytes")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reddit Post Crawler")

    parser.add_argument("--seed_file", type=str, default="seeds.txt", help="Path to seed file")
    parser.add_argument("--num_pages", type=int, default=None, help="Number of pages to collect (optional)")
    parser.add_argument("--size_limit", type=int, default=500*1024*1024, help="Total data size limit in bytes")
    parser.add_argument("--output_dir", type=str, default="output", help="Directory to store output JSON files")
    parser.add_argument("--num_procs", type=int, default=16, help="Number of parallel processes")
    parser.add_argument("--debug", action="store_true", help="Enable debug printing")
    parser.add_argument("--hops_away", type=int, default=1, help="Number of hops to follow from each seed post")
    parser.add_argument("--timeout", type=int, default=60, help="Timeout in seconds for each spider thread")

    args = parser.parse_args()

    crawler = Crawler(
        seed_file=args.seed_file,
        num_pages=args.num_pages,
        size_limit=args.size_limit,
        output_dir=args.output_dir,
        num_procs=args.num_procs,
        debug=args.debug,
        hops_away=args.hops_away,
        timeout=args.timeout
    )
