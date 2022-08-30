import os
import sys
import json

import logging
import PyPDF2

from config import *
from utils import isValidEmail

import pandas


logging.basicConfig(filename="debug.log", level=logging.DEBUG)

class DataDirectoryManager:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.dirs = self.get_dirs()

    def get_dirs(self):
        return os.listdir(self.data_dir)
    
    def path(self):
        return self.data_dir
    
    def get_dir(self):
        for dir in self.dirs:
            yield dir

    def __len__(self):
        return len(self.dirs)
    
class Reader:
    def __init__(self, path):
        self.path = path
        self.texts = ""
        self.data = {}
    
    def process(self):
        pass

class PDFreader(Reader):
    def __init__(self, path):
        super().__init__(path)
        self.fileReader = PyPDF2.PdfFileReader(path)
        self.num_pages = self.fileReader.getNumPages()
        
    
    def process(self):
        for i in range(self.num_pages):
            page = self.fileReader.pages[i]
            self.texts += page.extract_text()
        
        for line in self.texts.split("\n"):
            if ':' in line:
                keyvalue = line.split(":")
                self.data[keyvalue[0]] = keyvalue[1]
                
class JSONreader(Reader):
    def __init__(self, path):
        super().__init__(path)
    
    def process(self):
        with open(self.path, 'rb') as f:
            data = json.load(f)
        
        if 'data' in data:
            self.data = data['data']
            
def read_pdf(path):
    reader = PDFreader(path)
    reader.process()

    text = reader.texts
    data = reader.data

    type = data.get("Type", "")
    if type == "none":
        raise TypeError("Type not Found")

    name = data.get("Complete Name", "") if data.get("Name", "")=="" else data.get("Name", "")
    email = data.get("Email", "")
    account = data.get("Account", "")

    return str(name).strip(), str(email).strip().lower(), str(account).strip()

def read_json(path):
    reader = JSONreader(path)
    reader.process()
    
    data = reader.data

    name = ""
    email = ""
    account = ""

    try:
        backend = data['backend']
        user = backend['user']
        type = backend.get("type", "")
        if type!="":
            if "power_of_attorney" in type:
                name = user['name']
                email = user['email']
            else:
                logging.debug('{}: other type'.format(path))
        else:

            basic_information = data['basic_information']

            # search name from json
            if user.get('name', "") != "":
                name = user.get('name', "")
            elif basic_information.get('account_name', "") != "":
                name = basic_information.get('account_name', "")
            elif basic_information.get('first_name', "") != "" or basic_information.get('last_name', "") !="":
                name = " ".join([basic_information.get('first_name', ""), basic_information.get('last_name', "")])
            elif 'primary_holder' in data:
                primary_holder = data['primary_holder']
                name = " ".join([primary_holder.get('first_name', ""), primary_holder.get('last_name', "")])
            
            # search email from json
            if user.get('email', "")!="":
                email = user.get('email', "")
            elif basic_information.get('email', "")!="":
                email = basic_information.get('email', "")
            elif 'primary_holder' in data:
                primary_holder = data['primary_holder']
                email = primary_holder.get('email', "")

            # try to search account
            if "internal transfer" in data:
                pass
            if "platform" in data:
                platform = data["platform"]
                key = list(platform.keys())[0]
                try:
                    account = platform[key]['account']['login']
                except:
                    account = ""

    except Exception as e:
        logging.debug("Exception:{}:".format(e, path))

    return str(name).strip(), str(email).strip().lower(), str(account).strip()

def main():
    DDM = DataDirectoryManager(DATA_DIR)
    email_list = {}
    l = len(DDM)
    print(l)
    work_dir = DDM.path()
    result = []
    try:
        for i, dir in enumerate(DDM.dirs):
            print(i)
            full_path = os.path.join(work_dir, dir)
            files = os.listdir(full_path)

            name = ""
            email = ""
            account = ""
            
            if(dir+".json" in files):
                # read json
                # if json present, read json
                path = os.path.join(DDM.path(), dir, dir+".json")
                name, email, account = read_json(path)

                # check if there is pdf file in the directory
                if email == "":
                    pdfs = [i for i in files if i.endswith(".pdf")]
                    if len(pdfs) > 0:
                        for p in pdfs:
                            path = os.path.join(DDM.path(), dir, p)
                            p_name, p_email, p_account = read_pdf(path)
                            if name == "" and p_name != "":
                                name = p_name
                            if email == "" and p_email != "":
                                email = p_email
                            if account == "" and p_account != "":
                                account = p_account
                
                # still empty in the end
                if email == "":
                    logging.info("JSON Email Not Found, Please check: {}".format(path))

            elif(dir+".pdf" in files):
                path = os.path.join(DDM.path(), dir, dir+".pdf")
                name, email, account = read_pdf(path)
    
                if email == "":
                    logging.info("PDF Email not found: {}".format(path))

            else:
                logging.debug("No JSON or PDF present in folder: {}: {}".format(dir,
                    ",".join(os.listdir(os.path.join(DDM.path(), dir)))
                ))
            
            # check duplicate
            if email != "" and isValidEmail(email):
                if email in email_list:
                    # if orginal name and account are empty, update them
                    if email_list[email][1] == "":
                        email_list[email][1] = name
                    if email_list[email][3] == "":
                        email_list[email][3] = account
                else:
                    email_list[email] = [dir, name, email, account]

    except Exception as e:
        print(e)

    df = pandas.DataFrame(list(email_list.values()))
    logging.info(len(df.index))
    df.to_csv("result.csv", sep=",", header=False, index=False)


if __name__ == "__main__":
    main()