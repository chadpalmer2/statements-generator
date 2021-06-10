import os
from flask import Flask, render_template, request, redirect, url_for, abort, send_file
from werkzeug.utils import secure_filename

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

# Features to be added:
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
    def __init__(self, system, prices, broker_rate, agg_rate): # Initializes QuarterData instance
        self.err = False # Error reporting to main
        self.errmsg = ""

        self.system = "nepool" if system == 0 else "pjm"

        self.today = datetime.datetime.now().strftime("%m/%d/%Y")
        self.broker_rate = broker_rate
        self.agg_rate = agg_rate

        self.prices = prices

        self.period = None
        self.customer_data = {}
        self.directory = None

    def dp_report_err(self, message):
        self.err = True
        self.errmsg = message
        print(message)

    def add_nepool_data(self, production_file):
        # Read and validate files
        try:
            df = pd.read_csv(production_file)
        except:
            self.dp_report_err("Error: Production file not CSV")
            return

        required_headers = {"PeriodEndDate", "SystemID", "SysOwnerFirstName", "SysOwnerLastName", "SREC Program", "EnergyProduced"}
        if not required_headers.issubset(df.columns):
            self.dp_report_err("Correct CSV headers not present")
            return

        # Sets period (quarter and year) for all rows
        date = df["PeriodEndDate"].iloc[0].split("/")

        if self.period == None:
            self.period = f"Q{(((int(date[0]) - 2) % 12) // 3) + 1} {date[2]}"
        else:
            self.period = f"{ self.period }, Q{(((int(date[0]) - 2) % 12) // 3) + 1} {date[2]}"

        # Iterate over dictionary, surmising data
        for index, row in df.iterrows():
            id = row["SystemID"]
            if id not in self.customer_data:
                new_dict = {}

                new_dict["id"] = id
                new_dict["name"] = f"{row['SysOwnerFirstName']} {row['SysOwnerLastName']}"
                new_dict["generation"] = 0
                new_dict["statetype"] = row['SREC Program']

                self.customer_data[id] = new_dict

            self.customer_data[id]["generation"] += row["EnergyProduced"]

    def add_pjm_data(self, production_file, details_df):
        # Read and validate files
        try:
            df = pd.read_csv(production_file)
        except:
            self.dp_report_err("Error: Production file not CSV")
            return

        required_headers = {"Month of Generation", "Facility Name", "GATS Gen ID", "Generation (kWh)"}
        if not required_headers.issubset(df.columns):
            self.dp_report_err("Correct headers not present in Production CSV")
            return

        required_details_headers = {"GATS Unit ID", "State"}
        if not required_details_headers.issubset(details_df.columns):
            self.dp_report_err("Correct headers not present in My Generator Details CSV")
            return

        # Sets period (month and year) for all rows
        date = df["Month of Generation"].iloc[0].split("/")
        month_index = int(date[0]) - 1
        months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

        if self.period == None:
            self.period = f"{ months[month_index] } { date[2] }"
        else:
            self.period = f"{ self.period }, { months[month_index] } { date[2] }"

        # Iterates and processes rows
        for index, row in df.iterrows():
            id = row["GATS Gen ID"]
            if id not in self.customer_data:
                new_dict = {}

                new_dict["id"] = id
                new_dict["name"] = row["Facility Name"].split(" - ")[0]
                new_dict["generation"] = 0

                details_row = details_df.loc[details_df["GATS Unit ID"] == id]
                if len(details_row) < 1:
                    print(f"Warning: no row found in details CSV for { id }")
                    continue
                state = details_row.iloc[0]["State"]
                if state == "NY":
                    state = "DC" # NY SRECs are sold in DC market per Lee
                new_dict["statetype"] = state

                self.customer_data[id] = new_dict

            self.customer_data[id]["generation"] += int(row["Generation (kWh)"].replace(",", ""))

    def filter_ids(self, id_file): # Filters production data with ids from csv
        try:
            df = pd.read_csv(id_file)
        except:
            self.dp_report_err("Error: ID file not CSV")
            return

        ids = df.loc[:,"SystemID"].to_list()

        for key in self.customer_data.copy().keys():
            if int(key.split("-")[2]) not in ids:
                self.customer_data.pop(key)

    def build_pdf(self, template_path, filepath, pdf_dict):
        temp = ""   # Temporary string where HTML template is read and modified
        with open(template_path, "r") as f:
            temp = f.read()
        
        # Filling template
        temp = temp.replace("{{ path }}", pdf_dict["path"]) # image path
        temp = temp.replace("{{ date }}", pdf_dict["date"])
        temp = temp.replace("{{ period }}", pdf_dict["period"])
        temp = temp.replace("{{ name }}", pdf_dict["name"])
        temp = temp.replace("{{ id }}", pdf_dict["id"])
        temp = temp.replace("{{ generation }}", pdf_dict["generation"])
        temp = temp.replace("{{ price }}", pdf_dict["price"])
        temp = temp.replace("{{ subtotal }}", pdf_dict["subtotal"])
        temp = temp.replace("{{ broker_rate }}", pdf_dict["broker_rate"])
        temp = temp.replace("{{ broker_payment }}", pdf_dict["broker_payment"])
        temp = temp.replace("{{ agg_rate }}", pdf_dict["agg_rate"])
        temp = temp.replace("{{ aggregator }}", pdf_dict["aggregator"])
        temp = temp.replace("{{ payment }}", pdf_dict["payment"])
        
        # Generating PDF file
        erase_file_if_present(filepath)
    
        pdfkit.from_string(
            temp, 
            filepath,
            options=OPTIONS
        )

    def build_files(self):
        directory = f"{pathlib.Path().absolute()}/tmp/{self.period}_statements"
        if not create_directory(directory):
            self.dp_report_err("Error: Directory creation failed")
            return

        template_path = f"{pathlib.Path().absolute()}/rss/statement_template.html"
        photo_path = f"{pathlib.Path().absolute()}/rss/logo.png"
        check_data = []

        unfinished_count = len(self.customer_data)
        finished_count = 0
        print(f"Saving {unfinished_count} statements...")

        for customer in self.customer_data.values():
            pdf_dict = {}

            pdf_dict["path"] = photo_path
            pdf_dict["date"] = self.today
            pdf_dict["period"] = self.period

            pdf_dict["name"] = customer["name"]
            pdf_dict["id"] = customer["id"]

            # Calculating template values
            generation = customer["generation"] / 1000
            price = self.prices[customer["statetype"]]
            subtotal = price * generation
            broker_payment = self.broker_rate * generation
            aggregator = self.agg_rate * subtotal
            payment = subtotal - broker_payment - aggregator

            pdf_dict["generation"] = f"{generation:,.4f}"
            pdf_dict["price"] = f"{price:,.2f}"
            pdf_dict["subtotal"] = f"{subtotal:,.2f}"
            pdf_dict["broker_rate"] = f"{self.broker_rate:,.2f}"
            pdf_dict["broker_payment"] = f"{broker_payment:,.2f}"
            pdf_dict["agg_rate"] = f"{(self.agg_rate * 100):,.2f}"
            pdf_dict["aggregator"] = f"{aggregator:,.2f}"
            pdf_dict["payment"] = f"{payment:,.2f}"

            filename = f"{customer['name'].replace(' ', '_')}_{customer['id']}"
            filepath = f"{directory}/{filename}_statement.pdf"

            self.build_pdf(template_path, filepath, pdf_dict)

            # Appending data for checking CSV
            check_data.append([self.today, customer["name"], f"{payment:,.2f}"])

            finished_count += 1
            unfinished_count -= 1
            print(f"Statement {finished_count} complete! ({unfinished_count} remaining)")
        
        # Building empty template
        print("Building empty template")
        empty_pdf_dict = {
            "path": photo_path,
            "date": "",
            "period": "",
            "name": "",
            "id": "",
            "generation": "",
            "price": "",
            "subtotal": "",
            "broker_rate": "",
            "broker_payment": "",
            "agg_rate": "",
            "aggregator": "",
            "payment": "",
        }
        emptyname = "template"
        emptypath = f"{directory}/{emptyname}_statement.pdf"
        self.build_pdf(template_path, emptypath, empty_pdf_dict)

        # Building checking CSV
        print("Building checking CSV")
        df = pd.DataFrame(check_data, columns = ['Date', 'Name', 'Amount'])
        filepath = f"{directory}/checking.csv"
        if os.path.exists(filepath):
            os.remove(filepath)
        df.to_csv(filepath, index=False)
        print("Done")

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
        return redirect(url_for('error', msg="Error: Temporary folder could not be built (permissions error?)"))

    system = get_system_from_form(request.form)
    if system == -1:
        return redirect(url_for('error', msg="Error: No system (NEPool or PJM) Selected"))
    elif system == 0:
        prices = {
            1: get_from_form(request.form, "price_1"),
            2: get_from_form(request.form, "price_2")
        }
    else:
        prices = {
            "DC": get_from_form(request.form, "price_dc"),
            "NJ": get_from_form(request.form, "price_nj"),
            "MD": get_from_form(request.form, "price_md")
        }

    if -1 in prices.values():
        return redirect(url_for('error', msg="Error: Form incomplete (price not entered)"))

    broker_rate = get_from_form(request.form, "broker_rate")
    agg_rate = get_from_form(request.form, "agg_rate")

    if broker_rate == -1 or agg_rate == -1:
        return redirect(url_for('error', msg="Error: Form incomplete (no broker rate or no aggregator rate)"))

    dp = DataProcessor(system, prices, broker_rate, agg_rate) # Instantiating dp instance
    if dp.err:
        return redirect(url_for('error', msg="Error: DataProcessor failed to instantiate"))

    if request.files['prod_file_1'].filename == '':
        return redirect(url_for('error', msg="Error: Form incomplete (no Production CSV uploaded)"))

    prod_files = []
    prod_files.append(request.files['prod_file_1']) # Get file from form

    if request.files['prod_file_2'].filename != '':
        prod_files.append(request.files['prod_file_2'])

    if request.files['prod_file_3'].filename != '':
        prod_files.append(request.files['prod_file_3'])

    if dp.system == "nepool":
        for prod_file in prod_files:
            dp.add_nepool_data(prod_file) # Populates qd with data
    else:
        if request.files['details_file'].filename == '':
            return redirect(url_for('error', msg="Error: Form incomplete (no Generator Details CSV uploaded)"))

        details_file = request.files['details_file']

        try:
            details_df = pd.read_csv(details_file)
        except:
            return redirect(url_for('error', msg="Error: My Generator Details file not CSV"))

        for prod_file in prod_files:
            dp.add_pjm_data(prod_file, details_df)

    if dp.err:
        return redirect(url_for('error', msg="Error: Production data CSV or My Generator Details CSV improperly formatted or corrupted"))

    if not request.files['id_file'].filename == '': # Optionally filters qd with ids from other file
        id_file = request.files['id_file']
        dp.filter_ids(id_file)
        if dp.err:
            return redirect(url_for('error', msg="Error: ID CSV improperly formatted or corrupted"))

    dp.build_files() # Constructs PDFs and CSV
    if dp.err:
        return redirect(url_for('error', msg="Error: File building failed"))

    print(f"Statements successfully generated!")
    return send_file(dp.directory + ".zip", as_attachment=True) # Downloads zip file through browser

@app.route('/error/<msg>') # Error messaging page
def error(msg):
    print(msg)
    return render_template('error.html', msg=msg)

@app.route('/guide/') # Help page
def guide():
    return render_template('guide.html')

@app.route('/<other>/') # Catch-all to redirect to index
def other(other):
    return redirect(url_for('index'))