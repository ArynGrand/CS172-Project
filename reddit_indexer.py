import os
import json
import argparse
import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import (
    Document,
    StringField,
    TextField,
    LongPoint,
    StoredField
)
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import FSDirectory

class RedditIndexer:
    def __init__(self, json, index, debug=False):
        self.json = json
        self.index = index
        self.debug = debug
        os.makedirs(self.index, exist_ok=True)

        #Now we start the lucene
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        if self.debug:
            print("Lucene Initialized")

        #create the Lucene index, got from online
        conf = IndexWriterConfig(StandardAnalyzer())
        conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        #creating a new index
        self.writer = IndexWriter(FSDirectory.open(Paths.get(self.index)), conf)

    def json_indexes(self):
        document_count = 0
        for json_file in os.listdir(self.json):      
            if not json_file.endswith('.jsonl'):
                continue
            #get the path of the json
            json_path = os.path.join(self.json, json_file)
            if not os.path.exists(json_path):
                continue
                
            if self.debug:
                print(f"Processing file: {json_file}")
                
            with open(json_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f):
                    try:
                        line = line.strip()
                        if not line:
                            continue
                        data = json.loads(line)
                        #create document and add fields
                        document = Document()
                        document.add(StringField("id", str(data.get("id", "")), StringField.Store.YES))
                        document.add(StringField("subreddit", str(data.get("subreddit", "")), StringField.Store.YES))
                        document.add(StringField("author", str(data.get("author", "")), StringField.Store.YES))
                        document.add(StringField("url", str(data.get("url", "")), StringField.Store.YES))
                        document.add(TextField("title", str(data.get("title", "")), TextField.Store.YES))
                        document.add(TextField("selftext", str(data.get("selftext", "")), TextField.Store.YES))
                        #handle timestamp
                        timestamp = int(data.get("created_utc", 0))
                        document.add(LongPoint("timestamp", timestamp))
                        document.add(StoredField("timestamp_stored", timestamp))
                        #handle comments
                        comments = data.get("comments", [])
                        if comments:
                            comments_text = " ".join(str(c) for c in comments)
                            document.add(TextField("comments", comments_text, TextField.Store.NO))
                        #handle external links
                        external_links = data.get("external_links", [])
                        if external_links:
                            links_text = " ".join(str(link) for link in external_links)
                            document.add(StringField("external_links", links_text, StringField.Store.YES))
                        
                        self.writer.addDocument(document)
                        document_count += 1
                        
                        if self.debug and document_count % 100 == 0:
                            print(f"Indexed {document_count} documents")
                            
                    except json.JSONDecodeError as e:
                        if self.debug:
                            print(f"JSON decode error in {json_file} line {line_num + 1}: {e}")
                    except Exception as e:
                        if self.debug:
                            print(f"Error processing document in {json_file} line {line_num + 1}: {e}")
        
        self.writer.close()
        print(f"Indexing complete. Total documents indexed: {document_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reddit Data Indexer")
    parser.add_argument("--json", type=str, default="output", help="Directory containing JSON files")
    parser.add_argument("--index", type=str, default="index", help="Directory to store Lucene index")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.json):
        print(f"Error: JSON directory {args.json} does not exist")
        exit(1)
    
    indexer = RedditIndexer(args.json, args.index, args.debug)
    indexer.json_indexes()
