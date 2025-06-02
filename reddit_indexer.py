import os
import json
import argparse
import lucene
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

class redditIndexer:
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
        self.writer = IndexWriter(FSDirectory.open(Paths.get(index_dir)), conf)
    def json_indexes(self):
        for json_file in os.listdir(self.json):      
            #get the path of the json
            json_path = os.path.join(self.json, json_file)
            if not os.path.exists(json_path):
                 continue
            with open(json_path, "r", encoding="utf-8") as f:
                 for line in f:
                      data = json.loads(line)
                      #adding documents to the index
                      document = Document()
                      document.add(StringField("author", data.get("author", ""), StringField.Store.YES))
                      document.add(StringField("timestamp", data.get("created_utc", ""), StringField.Store.YES))
                      document.add(StringField("selftext", data.get("selftext", ""), StringField.Store.YES))
                      document.add(TextField("comments", "\n".join(data.get("comments", []), TextField.Store.NO)))
                      self.writer.addDocument(document)
        self.writer.close()


