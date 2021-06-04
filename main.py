import os
from flask import Flask, render_template, request, redirect, url_for, abort, send_file
from werkzeug.utils import secure_filename

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

# Confusions:
    # 39 pdfs rather than 41 - two accounts not in masscec-pts system?

# Issues:
    # No error checking if csv columns don't exist AKA csv is not properly formatted

# Features to be added:
    # Finish incorporating functionality for PJM as well as NEPool
    # add functionality for check writing
    # Reflect error messaging on error page, rather than terminal

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
    def __init__(self, price_1, price_2, broker_rate, agg_rate, quarterly=True): # Initializes QuarterData instance
        if quarterly == False:
            print("PJM processing not yet supported")
            self.err = True
            return

        self.err = False # Error reporting to main
        self.quarterly = quarterly

        self.today = datetime.datetime.now().strftime("%m/%d/%Y")
        self.broker_rate = broker_rate
        self.agg_rate = agg_rate
        self.price_1 = price_1
        self.price_2 = price_2

        self.period = None
        self.customer_data = {}
        self.directory = None

    def add_production_data(self, production_file): # Adds production data to customer_data from csv
        try:
            df = pd.read_csv(production_file)
        except:
            print("Error: Production file not CSV")
            self.err = True
            return

        # Sets period (quarter and year) for all rows
        date = df["PeriodEndDate"].iloc[0].split("/")
        self.period = f"Q{(((int(date[0]) - 2) % 12) // 3) + 1} {date[2]}"

        for index, row in df.iterrows(): # Iterate over dictionary, surmising data
            id = row["SystemID"]
            if id not in self.customer_data:
                new_dict = {}

                new_dict["id"] = id
                new_dict["name"] = f"{row['SysOwnerFirstName']} {row['SysOwnerLastName']}"
                new_dict["generation"] = 0
                new_dict["srec_type"] = row['SREC Program']

                self.customer_data[id] = new_dict

            sys_energy = row["EnergyProduced"]
            self.customer_data[id]["generation"] += sys_energy

    def filter_ids(self, id_file): # Filters production data with ids from csv
        try:
            df = pd.read_csv(id_file)
        except:
            print("Error: ID file not CSV")
            self.err = True
            return

        ids = df.loc[:,"SystemID"].to_list()

        for key in self.customer_data.copy().keys():
            if int(key.split("-")[2]) not in ids:
                self.customer_data.pop(key)


    def build_files(self):
        directory = f"{pathlib.Path().absolute()}/tmp/{self.period}_statements"
        if not create_directory(directory):
            print("Error: Directory creation failed")
            self.err = True
            return

        template_path = f"{pathlib.Path().absolute()}/rss/statement_template.html"
        photo_path = f"{pathlib.Path().absolute()}/rss/logo.png"
        check_data = []

        unfinished_count = len(self.customer_data)
        finished_count = 0
        print(f"Saving {unfinished_count} statements...")

        for customer in self.customer_data.values():
            
            temp = ""   # Temporary string where HTML template is read and modified
            with open(template_path, "r") as f:
                temp = f.read()

            # Calculating template values
            generation = customer["generation"] / 1000
            price = self.price_1 if customer["srec_type"] == 1 else self.price_2
            subtotal = price * generation
            brokerpayment = self.broker_rate * generation
            aggregator = self.agg_rate * subtotal
            payment = subtotal - brokerpayment - aggregator
            
            # Filling template
            temp = temp.replace("{{ path }}", photo_path) # image path
            temp = temp.replace("{{ date }}", self.today)
            temp = temp.replace("{{ period }}", self.period)

            temp = temp.replace("{{ name }}", customer["name"])
            temp = temp.replace("{{ id }}", customer["id"])

            temp = temp.replace("{{ generation }}", f"{generation:,.4f}")
            temp = temp.replace("{{ price }}", f"{price:,.2f}")
            temp = temp.replace("{{ subtotal }}", f"{subtotal:,.2f}")
            temp = temp.replace("{{ broker_rate }}", f"{self.broker_rate:,.2f}")
            temp = temp.replace("{{ brokerpayment }}", f"{brokerpayment:,.2f}")
            temp = temp.replace("{{ agg_rate }}", f"{(self.agg_rate * 100):,.2f}")
            temp = temp.replace("{{ aggregator }}", f"{aggregator:,.2f}")
            temp = temp.replace("{{ payment }}", f"{payment:,.2f}")
            
            # Generating PDF file
            filename = f"{customer['name'].replace(' ', '_')}_{customer['id']}"
            filepath = f"{directory}/{filename}_statement.pdf"
            erase_file_if_present(filepath)
        
            pdfkit.from_string(
                temp, 
                filepath,
                options=OPTIONS
            )

            # Appending data for checking CSV
            check_data.append([self.today, customer["name"], f"{payment:,.2f}"])

            finished_count += 1
            unfinished_count -= 1
            print(f"Statement {finished_count} complete! ({unfinished_count} remaining)")
        
        # Building checking CSV
        df = pd.DataFrame(check_data, columns = ['Date', 'Name', 'Amount'])
        filepath = f"{directory}/checking.csv"
        if os.path.exists(filepath):
            os.remove(filepath)
        df.to_csv(filepath, index=False)

        zip_directory(directory)
        self.directory = directory

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

    price_1 = get_from_form(request.form, "price_1") # Get values from form
    price_2 = get_from_form(request.form, "price_2")
    broker_rate = get_from_form(request.form, "broker_rate")
    agg_rate = get_from_form(request.form, "agg_rate")
    system = get_system_from_form(request.form)

    if request.files['prod_file'].filename == '' or price_1 == -1 or price_2 == -1 or broker_rate == -1 or agg_rate == -1 or system == -1:
        print("Error: Form incomplete")
        return redirect(url_for('error'))

    prod_file = request.files['prod_file'] # Get file from form

    dp = DataProcessor(price_1, price_2, broker_rate, agg_rate, quarterly=(system == 0)) # Instantiating dp instance
    if dp.err == True:
        print("Error: DataProcessor failed to instantiate")
        return redirect(url_for('error'))

    dp.add_production_data(prod_file) # Populates qd with data
    if dp.err == True:
        print("Error: Production data CSV improperly formatted or corrupted")
        return redirect(url_for('error'))

    if not request.files['id_file'].filename == '': # Optionally filters qd with ids from other file
        id_file = request.files['id_file']
        dp.filter_ids(id_file)
        if dp.err == True:
            print("Error: ID CSV improperly formatted or corrupted")
            return redirect(url_for('error'))

    dp.build_files() # Constructs PDFs and CSV
    if dp.err == True:
        print("Error: File building failed")
        return redirect(url_for('error'))

    print(f"Statements successfully generated!")
    return send_file(dp.directory + ".zip", as_attachment=True) # Downloads zip file through browser

@app.route('/error')
def error():
    return render_template('error.html')

@app.route('/<other>/') # Catch-all to redirect to index
def other(other):
    return redirect(url_for('index'))