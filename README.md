# Statements Generator

A simple webpage that converts a CSV file into pdf statements.

## First-time Setup

Note that these instructions are for macOS. Skip any of these steps if you have previously installed that software.

1.	Install Homebrew

Homebrew is a package manager for macOS that makes getting useful software for programming and development easier. A guide for installing it can be found here. The guide, summarized, is as follows. Open a Terminal window and run the following commands:

```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
nano ~/.profile
export PATH="/usr/local/opt/python/libexec/bin:$PATH"
```

2.	Install Python and wkhtmltopdf

Python is an immensely popular programming language which this generator is written in (thanks to Shaun Radgowski for his initial development!). wkhtmltopdf is a software package that converts HTML documents to PDF. In a Terminal window, run:

```
brew install python
brew install wkhtmltopdf --cask
```

3.	Install Chad’s Statement Generator

In a Terminal window, run:

```
cd Desktop
git clone https://github.com/chadpalmer2/statements-generator.git
cd statements-generator
source venv/scripts/activate
pip install -r requirements.txt
```

Close out of the Terminal window, and you’re done!

## Usage

Open the statements-generator folder on your Desktop, and double click on wsgi.py. This will launch a Terminal window, with a few lines of text. The final line will have a URL, which you can access with the web browser of your choice to use the Generator. When you are done, press Command+C in the Terminal window to close the software.

## Technical Details

Chad’s Statement Generator is a Flask web application which provides a convenient GUI for inputting a couple of CSV files and parameters into a Python script. Roughly speaking, this script loads the data in the CSV file into a pandas dataframe, iterates over this dataframe to calculate necessary statement data, generates an HTML document from a template using that data, converts this to a PDF with pdfkit, and finally zips that file.

All of the packages necessary for this application to run are pip-installable and installed via requirements.txt, with the exception of wkhtmltopdf. The file tree is as follows:

```
-	/rss 		    # template and logo for statements
-	/templates 		# HTML templates for rendering views in Flask
-	/venv 		    # virtual Python environment
-	main.py		    # all project code
-	Procfile		# holdover from when I intended to deploy application on Heroku
-	README.md		# readme containing this information
-	requirements.txt 	# necessary Python packages, pip-installable
-	wsgi.py		    # wsgi endpoint for running server	
```
