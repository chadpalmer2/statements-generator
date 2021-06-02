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

# Features to be added:
    # functionality for "by-month"
    # functionality for PJM as well as NEPool
    # add functionality for check writing

def nameify(name):
    result = name[0]
    for i, char in enumerate(name[1:]):
        if char.isupper():
            if name[i] != " ":
                result += " "
        result += char

    return result

def zip_directory(directory): # Zip local directory with path "directory" with error checking. Deletes first if already present
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

def get_from_form(form, value): # Gets string value from submitted form with error checking
    result = form[value]
    try:
        result = float(result)
    except ValueError:
        print("Error: Price should be a decimal number")
        return -1
    if result < 0:
        print("Error: Price should be nonnegative")
        return -1
    
    return result

class QuarterData():
    def __init__(self, uploaded_file, price, broker_rate, agg_rate): # Initializes QuarterData instance
        self.broker_rate = broker_rate
        self.agg_rate = agg_rate
        self.price = price
        self.df = pd.read_csv(uploaded_file) # Should exit if file open fails
        self.systems = defaultdict(int)

        # use .split instead
        date = self.df["PeriodEndDate"].iloc[0]
        first_slash = date.find("/")
        month = date[:first_slash]
        second_slash = date.find("/", first_slash + 1)
        self.year = date[second_slash + 1:second_slash + 5]

        # will all data be grouped by quarter? seems to assume so
        if month in ["2", "3", "4"]:
            self.quarter = 1

        elif month in ["5", "6", "7"]:
            self.quarter = 2

        elif month in ["8", "9", "10"]:
            self.quarter = 3

        elif month in ["11", "12", "1"]:
            self.quarter = 4

        else:
            print(f"Error: Period End Date not valid (month: { month } not 1-12)")
            return None

        statements_path = f"{pathlib.Path().absolute()}/tmp/Q{self.quarter}_statements"
        if not create_directory(statements_path):
            return None
        self.path = statements_path

        self.filter_ids = None

    def add_filter_ids(self, id_csv): # Adds filter_ids field for filtering based on optional csv
        id_df = pd.read_csv(id_csv)
        self.filter_ids = id_df.loc[:,"SystemID"].to_list

    def fill(self): # Iterates over rows, adding data together and populating self.systems
        for index, row in self.df.iterrows():
            sys_id = row["SystemID"]
            sys_energy = row["EnergyProduced"]

            if self.filter_ids == None or (int(sys_id.split("-")[2]) in self.filter_ids()):
                self.systems[sys_id] += sys_energy

    def build_pdfs(self): # Bulids HTML for file, converts to PDF with pdfkit
        length = len(self.systems)
        print("")
        print(f"Saving {length} statements...\n")

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

            rows = self.df.loc[self.df["SystemID"] == system, "SystemName"]
            name = nameify(rows.iloc[0]) # why only factor out one single csv read into a helper function? all or none
            
            temp = temp.replace("{{ path }}", path) # image path
            temp = temp.replace("{{ date }}", f"{today}")
            temp = temp.replace("{{ quarter }}", f"Q{self.quarter} {self.year}")
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
            if os.path.exists(filepath):
                os.remove(filepath)
        
            pdfkit.from_string(
                temp, 
                filepath,
                options=OPTIONS
            )

            print(f"Statement {i + 1} complete! ({length - i - 1} remaining)")

    def zip(self): # Zips PDF directory
        zip_directory(self.path)

    def run(self):
        self.fill()
        self.build_pdfs()
        self.zip()

## routes

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/', methods=['POST'])
def upload_file():
    tmp_path = f"{pathlib.Path().absolute()}/tmp"

    if not create_directory(tmp_path):
        return redirect(url_for('index'))

    price = get_from_form(request.form, "price")
    if price == -1:
        return redirect(url_for('index'))

    broker_rate = get_from_form(request.form, "broker_rate")
    if broker_rate == -1:
        return redirect(url_for('index'))

    agg_rate = get_from_form(request.form, "agg_rate")
    if agg_rate == -1:
        return redirect(url_for('index'))

    if request.files['prod_file'].filename == '':
        print("Error: No Production Data file")
        return redirect(url_for('index'))
    prod_file = request.files['prod_file']

    qd = QuarterData(prod_file, price, broker_rate, agg_rate)
    if qd == None:
        return redirect(url_for('index'))

    if not request.files['id_file'].filename == '':
        id_file = request.files['id_file']
        qd.add_filter_ids(id_file)

    qd.run()

    print(f"\nStatements successfully generated!")
    return send_file(qd.path + ".zip", as_attachment=True)

@app.route('/<other>/') # Catch-all to redirect to index
def other(other):
    return redirect(url_for('index'))