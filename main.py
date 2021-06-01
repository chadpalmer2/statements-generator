import os
from flask import Flask, render_template, request, redirect, url_for, abort, send_file
from werkzeug.utils import secure_filename

from collections import defaultdict
import datetime
import pandas as pd # csv library more lightweight for use case
import pathlib
import pdfkit

from zipfile import ZipFile
import shutil

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024
app.config['UPLOAD_EXTENSIONS'] = ['.csv']
app.config['UPLOAD_PATH'] = 'uploads'

# class
OPTIONS = {
  "enable-local-file-access": "",
  "quiet": ""
}

# problems with current app:
    # no evidence of action on user side - progress bar via js? redirect to screen with report?
    # not pushed to online access anywhere - heroku dynos might destroy it
    # read from list of wanted ids
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

def get_all_file_paths(directory):
    file_paths = []
  
    for root, directories, files in os.walk(directory):
        for filename in files:
            #filepath = os.path.join(root, filename)
            file_paths.append(filename)
  
    return file_paths        

# class based approach is clumsy here
class QuarterData():
    def __init__(self, uploaded_file, price, broker_rate, agg_rate): # initializes price, year, quarter
        self.path = ""
        
        self.broker_rate = broker_rate
        self.agg_rate = agg_rate
        self.price = price
        self.df = pd.read_csv(uploaded_file) # should exit if file open fails
        self.systems = defaultdict(int)

        # use .split instead
        date = self.df["Period End Date"].iloc[0]
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
            raise ValueError("Period End Date not valid")

    def fill(self): # iterates over rows, accumulating energy across ids
        for index, row in self.df.iterrows():
            sys_id = row["System ID"]
            sys_energy = row["Energy Produced"]
            self.systems[sys_id] += sys_energy

    def folder(self): # makes a folder  
        statements_path = f"{pathlib.Path().absolute()}/tmp/Q{self.quarter}_statements"

        if os.path.exists(statements_path):
            try:
                shutil.rmtree(statements_path)
            except OSError as e:
                print("Error: %s : %s" % (statements_path, e.strerror))
                exit(1)

        try:
            os.mkdir(statements_path)
        except OSError:
            print(f"Creation of the directory {statements_path} failed.")
            exit(1)

        self.path = statements_path

    def template(self):
        length = len(self.systems)
        print("")
        print(f"Saving {length} statements...\n")

        path = f"{pathlib.Path().absolute()}/rss/logo.png"
        print(path)
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

            rows = self.df.loc[self.df["System ID"] == system, "System Name"]
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

            filename = name.replace(" ", "_")
            filepath = f"{self.path}\{filename}_statement.pdf"
            if os.path.exists(filepath):
                os.remove(filepath)
        
            pdfkit.from_string(
                temp, 
                filepath,
                options=OPTIONS
            )

            print(f"Statement {i + 1} complete! ({length - i - 1} remaining)")

    def zip(self):
        dirPath = self.path
        zipPath = self.path + ".zip"

        if os.path.exists(zipPath):
            os.remove(zipPath)

        zipf = ZipFile(zipPath, mode='w')
        lenDirPath = len(dirPath)
        for root, _ , files in os.walk(dirPath):
            for file in files:
                filePath = os.path.join(root, file)
                zipf.write(filePath , filePath[lenDirPath :] )
        zipf.close() 

    def run(self):
        self.fill()
        self.folder()
        self.template()
        self.zip()
        return True # TODO: implement error checking here

## routes

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/asp-statements-generator')
def form_file():
    return render_template('generator.html')

@app.route('/asp-statements-generator', methods=['POST'])
def upload_file():
    tmp_path = f"{pathlib.Path().absolute()}/tmp"

    if os.path.exists(tmp_path):
        try:
            shutil.rmtree(tmp_path)
        except OSError as e:
            print("Error: %s : %s" % (tmp_path, e.strerror))
            return redirect(url_for('/asp-statements-generator'))

    try:
        os.mkdir(tmp_path)
    except OSError:
        print(f"Creation of the directory {tmp_path} failed.")
        return redirect(url_for('index'))

    price = request.form['price']
    try:
        price = float(price)
    except ValueError:
        print("Error: Price should be a decimal number")
        return redirect(url_for('index'))
    if price < 0:
        print("Error: Price should be nonnegative")
        return redirect(url_for('index'))

    broker_rate = request.form['broker_rate']
    try:
        broker_rate = float(broker_rate)
    except ValueError:
        print("Error: broker_rate should be a decimal number")
        return redirect(url_for('index'))
    if broker_rate < 0:
        print("Error: broker_rate should be nonnegative")
        return redirect(url_for('index'))

    agg_rate = request.form['agg_rate']
    try:
        agg_rate = float(agg_rate)
    except ValueError:
        print("Error: agg_rate should be a decimal number")
        return redirect(url_for('index'))
    if agg_rate < 0:
        print("Error: agg_rate should be nonnegative")
        return redirect(url_for('index'))

    if request.files['file'].filename == '':
        print("Error: No file")
        return redirect(url_for('index'))
    uploaded_file = request.files['file']

    schema = QuarterData(uploaded_file, price, broker_rate, agg_rate)

    if schema.run():
        print(f"\nStatements successfully generated!")
        return send_file(schema.path + ".zip", as_attachment=True)
    else:
        print("Failed to create statements")
        return redirect(url_for('index'))

@app.route('/<other>/')
def other(other):
    return redirect(url_for('index'))