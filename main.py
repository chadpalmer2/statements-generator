import os
from flask import Flask, render_template, request, redirect, url_for, abort, send_file
from werkzeug.utils import secure_filename

from collections import defaultdict
import datetime
import pandas as pd
import pathlib
import pdfkit
from zipfile import ZipFile
import shutil

import pdb

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 2048 * 2048
app.config['UPLOAD_EXTENSIONS'] = ['.csv']
app.config['UPLOAD_PATH'] = 'uploads'

OPTIONS = {
  "enable-local-file-access": "",
  "quiet": ""
}

# Issues:
    # 39 pdfs rather than 41 - two accounts not in masscec-pts system
    # No error checking if csv columns don't exist AKA csv is not properly formatted

# Features to be added:
    # functionality for "by-month"
    # functionality for PJM as well as NEPool
    # add functionality for check writing

# Helper functions

def zip_directory(directory): # Zip local directory with path "directory". Deletes first if already present
    dirPath = directory
    zipPath = directory + ".zip"

    if os.path.exists(zipPath):
        os.remove(zipPath)

    zipf = ZipFile(zipPath, mode='w')
    lenDirPath = len(dirPath)
    for root, _ , files in os.walk(dirPath):
        for file in files:
            filePath = os.path.join(root, file)
            zipf.write(filePath , filePath[lenDirPath :] )
    zipf.close() 

def create_directory(directory): # Creates local directory with path "directory" with error checking. Deletes first if already present
    if os.path.exists(directory):
        try:
            shutil.rmtree(directory)
        except OSError as e:
            print("Error: %s : %s" % (directory, e.strerror))
            return False

    try:
        os.mkdir(directory)
    except OSError:
        print(f"Creation of the directory {directory} failed.")
        return False
    
    return True

def erase_file_if_present(path): # Erases local file with path "path" if present.
    if os.path.exists(path):
        os.remove(path)


def get_from_form(form, value): # Gets string value from submitted form with error checking
    result = form[value]
    try:
        result = float(result)
    except ValueError:
        print("Error: Inputs should be decimal numbers")
        return -1
    if result < 0:
        print("Error: Inputs should be nonnegative")
        return -1
    
    return result

def get_system_from_form(form):
    result = ""
    try:
        result = form['system']
    except:
        print("Error: No system selected")
        return -1

    if result == "nepool":
        return 0
    elif result == "pjm":
        return 1
    else:
        print("Error: Bad form submission")
        return -1

class DataProcessor(): # Class used for processing NEPool quarterly data and PJM monthly data
    def __init__(self, production_file, price, broker_rate, agg_rate, quarterly=True): # Initializes QuarterData instance
        self.err = False
        
        self.broker_rate = broker_rate
        self.agg_rate = agg_rate
        self.price = price

        self.df = None

        try:
            self.df = pd.read_csv(production_file)
        except:
            print("Error: Production file not CSV")
            self.err = True
            return
        
        self.systems = defaultdict(int)

        date = self.df["PeriodEndDate"].iloc[0].split("/")
        self.year = date[2]
        self.quarter = (((int(date[0]) - 2) % 12) // 3) + 1 # Sets quarter by arithmetic on month

        statements_path = f"{pathlib.Path().absolute()}/tmp/Q{self.quarter}_statements"
        if not create_directory(statements_path):
            print("Error: Directory creation failed")
            self.err = True
            return

        self.path = statements_path
        self.ids = None

    def add_filter_ids(self, id_csv): # Adds ids field for filtering based on optional csv
        id_df = None
        try:
            id_df = pd.read_csv(id_csv)
        except:
            print("Error: ID file not CSV")
            self.err = True
            return

        self.ids = id_df.loc[:,"SystemID"].to_list()

    def fill(self): # Iterates over rows, adding data together and populating self.systems
        for index, row in self.df.iterrows():
            sys_id = row["SystemID"]
            sys_energy = row["EnergyProduced"]

            if self.ids == None or (int(sys_id.split("-")[2]) in self.ids):
                self.systems[sys_id] += sys_energy

    def construct_files(self): # Builds HTML for file, converts to PDF with pdfkit, also builds CSV for checking
        length = len(self.systems)
        print("")
        print(f"Saving {length} statements...\n")

        check_data = []

        path = f"{pathlib.Path().absolute()}/rss/logo.png"
        for i, system in enumerate(self.systems):
            temp = ""
            with open(f"{pathlib.Path().absolute()}/rss/statement_template.html", "r") as f:
                temp = f.read()

            today = datetime.datetime.now().strftime("%m/%d/%Y")
            generation = self.systems[system] / 1000 # round to thousandths place for legibility
            subtotal = self.price * generation
            brokerpayment = self.broker_rate * generation
            aggregator = self.agg_rate * subtotal
            payment = subtotal - brokerpayment - aggregator

            name_search = self.df.loc[self.df["SystemID"] == system, ["SysOwnerFirstName", "SysOwnerLastName"]]
            name = name_search.iloc[0]["SysOwnerFirstName"] + " " + name_search.iloc[0]["SysOwnerLastName"]
            
            temp = temp.replace("{{ path }}", path) # image path
            temp = temp.replace("{{ date }}", f"{today}")
            temp = temp.replace("{{ period }}", f"Q{self.quarter} {self.year}")
            temp = temp.replace("{{ name }}", name)
            temp = temp.replace("{{ id }}", f"{system}")
            temp = temp.replace("{{ generation }}", f"{generation:,.3f}")
            temp = temp.replace("{{ price }}", f"{self.price:,.2f}")
            temp = temp.replace("{{ subtotal }}", f"{subtotal:,.2f}")
            temp = temp.replace("{{ brokerpayment }}", f"{brokerpayment:,.2f}")
            temp = temp.replace("{{ aggregator }}", f"{aggregator:,.2f}")
            temp = temp.replace("{{ payment }}", f"{payment:,.2f}")
            temp = temp.replace("{{ broker_rate }}", f"{self.broker_rate:,.2f}")
            temp = temp.replace("{{ agg_rate }}", f"{(self.agg_rate * 100):,.2f}")

            filename = name.replace(" ", "_") + "_" + system
            filepath = f"{self.path}\{filename}_statement.pdf"
            erase_file_if_present(filepath)
        
            pdfkit.from_string(
                temp, 
                filepath,
                options=OPTIONS
            )

            check_data.append([today, name, payment])

            print(f"Statement {i + 1} complete! ({length - i - 1} remaining)")
        
        df = pd.DataFrame(check_data, columns = ['Date', 'Name', 'Amount'])

        filepath = f"{self.path}\checking.csv"
        if os.path.exists(filepath):
            os.remove(filepath)

        df.to_csv(filepath, index=False)

    ## API - work on progress, relies on previously defined functions - reworking so that class can handle pjm more effectively

    # def __init__(self, production_file, price, broker_rate, agg_rate, quarterly=True): # Initializes QuarterData instance
    #     self.err = False # Error reporting to main
        
    #     self.today = datetime.datetime.now().strftime("%m/%d/%Y")

    #     self.broker_rate = broker_rate
    #     self.agg_rate = agg_rate
    #     self.price = price

    #     try:
    #         self.df = pd.read_csv(production_file)
    #     except:
    #         print("Error: Production file not CSV")
    #         self.err = True
    #         return
        
    #     self.systems = defaultdict(int)

    #     date = self.df["PeriodEndDate"].iloc[0].split("/")
    #     self.year = date[2]
    #     self.quarter = (((int(date[0]) - 2) % 12) // 3) + 1 # Sets quarter by arithmetic on month

    #     statements_path = f"{pathlib.Path().absolute()}/tmp/Q{self.quarter}_statements"
    #     if not create_directory(statements_path):
    #         print("Error: Directory creation failed")
    #         self.err = True
    #         return

    #     self.path = statements_path
    #     self.ids = None

    def add_production_data(self, prod_file):
        pass

    def filter_ids(self, id_file):
        self.add_filter_ids(id_file)

    def build_files(self):
        self.fill()
        self.construct_files()
        zip_directory(self.path)

# URL routing

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/', methods=['POST'])
def upload_file():

    tmp_path = f"{pathlib.Path().absolute()}/tmp" # Build temporary folder for generated files

    if not create_directory(tmp_path):
        print("Error: Temporary folder could not be built (permissions error?)")
        return redirect(url_for('error'))

    price = get_from_form(request.form, "price") # Get values from form
    broker_rate = get_from_form(request.form, "broker_rate")
    agg_rate = get_from_form(request.form, "agg_rate")
    system = get_system_from_form(request.form)

    if request.files['prod_file'].filename == '' or price == -1 or broker_rate == -1 or agg_rate == -1 or system == -1:
        print("Error: Form incomplete")
        return redirect(url_for('error'))

    prod_file = request.files['prod_file'] # Get file from form

    if system == 0:
        dp = DataProcessor(prod_file, price, broker_rate, agg_rate, quarterly=True) # Instantiating dp instance
    elif system == 1:
        print("Error: PJM not yet supported")
        dp = DataProcessor(prod_file, price, broker_rate, agg_rate, quarterly=False)
        return redirect(url_for('error'))

    if dp.err == True:
        print("Error: DataProcessor failed to instantiate correctly")
        return redirect(url_for('error'))

    dp.add_production_data(prod_file) # Populates qd with data
    if dp.err == True:
        print("Error: Production data CSV improperly formatted or corrupted")
        return redirect(url_for('error'))

    if not request.files['id_file'].filename == '': # Optionally filters qd with ids from other file
        id_file = request.files['id_file']
        dp.filter_ids(id_file)

    dp.build_files() # Constructs PDFs and CSV
    if dp.err == True:
        print("Error: File building failed")
        return redirect(url_for('error'))

    print(f"\nStatements successfully generated!")
    return send_file(dp.path + ".zip", as_attachment=True) # Downloads zip file through browser

@app.route('/error')
def error():
    return render_template('error.html')

@app.route('/<other>/') # Catch-all to redirect to index
def other(other):
    return redirect(url_for('index'))