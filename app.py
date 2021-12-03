from flask import Flask, request, render_template, redirect, session, url_for
import os
import sqlite3
import re
import mysql.connector
import pandas as pd
import pymssql
from mysql.connector import errorcode
from flask_session import Session

app = Flask(__name__)
app.secret_key = '@dkjgfjgfhkj jxbjljv kjxgvljklkj'
dir_path = os.path.dirname(os.path.realpath(__file__))
# currentlocation = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(dir_path, 'static', 'files')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
#app.config.update(dict(PREFERRED_URL_SCHEME = 'https'))

config = {
  'host':'akatsuki.database.windows.net',
  'user':'akatsuki',
  'password':'Rihani@123',
  'database':'akatsuki'
}

def get_https_url(item):
    return url_for(item, _external=True, _scheme='http')

@app.route('/',methods=['GET','POST'])
def homepage():
    msg=''
    if request.method == 'POST' and 'username' in request.form and 'password' in request.form:
        username = request.form['username']
        password = request.form['password']
        conn = pymssql.connect(server=config['host'], user=config['user'], password=config['password'], database=config['database'])
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE username = %s AND password = %s', (username, password, ))
        user = cur.fetchone()
        if user:
            session['loggedin'] = True
            session['username'] = username
            print("here")
            return redirect(get_https_url('profile'))
        else:
            # Account doesnt exist
            msg = 'Incorrect username/password!'
    return render_template("homepage.html",msg=msg)


@app.route('/logout')
def logout():
   session.pop('username', None)
   return render_template("homepage.html")


@app.route('/register', methods=['GET', 'POST'])
def register():
    msg = ''
    if request.method == 'POST' and 'username' in request.form and 'password' in request.form:
        username = request.form['username']
        password = request.form['password']
        email = request.form['email']
        conn = pymssql.connect(server=config['host'], user=config['user'], password=config['password'], database=config['database'])
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE username = %s', (username,))
        user = cur.fetchone()
        if user:
            msg = 'Account already exists!'
        elif not re.match(r'[^@]+@[^@]+\.[^@]+', email):
            msg = 'Invalid email address!'
        elif not re.match(r'[A-Za-z0-9]+', username):
            msg = 'Username must contain only characters and numbers!'
        elif not username or not password or not email:
            msg = 'Please fill out the form!'
        else:
            cur.execute('INSERT INTO users VALUES (%s, %s,%s)', (username,password, email,))
            conn.commit()
            session['loggedin'] = True
            session['username'] = username
            return redirect(get_https_url('profile'))
    return render_template("register.html",msg=msg)


@app.route('/profile',methods=['GET','POST'])
def profile():
    if 'loggedin' in session:
        return render_template('dashboard.html')
    return redirect(get_https_url('homepage'))

@app.route('/Search', methods=['GET','POST'])
def Search():
    msg = ''
    if request.method == 'POST' and 'search' in request.form :
        number = request.form['search']
        if not re.match(r'\d+', number):
             msg = "enter a valid household number"
        else:
            conn = pymssql.connect(server=config['host'], user=config['user'], password=config['password'], database=config['database'])
            cur = conn.cursor()
        # testquery="SELECT h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_, p.DEPARTMENT, p.COMMODITY,t.SPEND, t.UNITS, t.STORE_R, t.WEEK_NUM, t.YEAR, h.L, h.AGE_RANGE,h.MARITAL_STATUS,h.INCOME_RANGE, h.HOMEOWNER_DESC, h.HSHD_COMPOSITION, h.HH_SIZE, h.CHILDREN FROM households AS h RIGHT JOIN transactions AS t ON h.HSHD_NUM = t.HSHD_NUM RIGHT JOIN products AS p ON t.PRODUCT_NUM = p.PRODUCT_NUM where h.HSHD_NUM=%d"
            #args=[10]
            #cur.callproc('SingleHshdPull', args)
            cur.execute("SELECT h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_, p.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY, t.SPEND, t.UNITS, t.STORE_R, t.WEEK_NUM, t.YEAR, h.L, h.AGE_RANGE,h.MARITAL,h.INCOME_RANGE, h.HOMEOWNER, h.HSHD_COMPOSITION, h.HH_SIZE, h.CHILDREN FROM households AS h RIGHT JOIN transactions AS t ON h.HSHD_NUM = t.HSHD_NUM RIGHT JOIN products AS p ON t.PRODUCT_NUM = p.PRODUCT_NUM where h.HSHD_NUM=%s ORDER BY h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_, p.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY",(number,))
            data=cur.fetchall()
            if data:
                return render_template('Search.html', data= data)
            else:
                msg="Not Data Found for the input "
                return render_template('Search.html', msg=msg)
        return render_template('Search.html', msg=msg)
    else:
        conn = pymssql.connect(server=config['host'], user=config['user'], password=config['password'], database=config['database'])
        cur = conn.cursor()
        testquery="SELECT h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_, p.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY, t.SPEND, t.UNITS, t.STORE_R, t.WEEK_NUM, t.YEAR, h.L, h.AGE_RANGE,h.MARITAL,h.INCOME_RANGE, h.HOMEOWNER, h.HSHD_COMPOSITION, h.HH_SIZE, h.CHILDREN FROM households AS h RIGHT JOIN transactions AS t ON h.HSHD_NUM = t.HSHD_NUM RIGHT JOIN products AS p ON t.PRODUCT_NUM = p.PRODUCT_NUM where h.HSHD_NUM=10 ORDER BY h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_, p.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY"
        #args=[10]
        #cur.callproc('SingleHshdPull', args)
        cur.execute(testquery)
        data=cur.fetchall()
        return render_template('Search.html', data= data)


@app.route('/dashboard')
def dashboard():
   return render_template("dashboard.html")

@app.route('/datastats')
def datastats():
   return render_template("datastats.html")

@app.route('/data1')
def data1():
   return render_template("data1.html")

@app.route('/data2')
def data2():
   return render_template("data2.html")

def execute_many(conn, datafrm, table):
    # Creating a list of tupples from the dataframe values
    tpls = [tuple(x) for x in datafrm.to_numpy()]

    # dataframe columns with Comma-separated
    cols = ','.join(list(datafrm.columns))

    # SQL query to execute
    sql = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        cursor.executemany(sql, tpls)
        conn.commit()
        print("Data inserted using execute_many() successfully...")
    except Exception as e:
        print("Error while inserting to MySQL", e)
        cursor.close()

def batch(x, bs):
    return [x[i:i+bs] for i in range(0, len(x), bs)]

@app.route('/upload', methods=['GET','POST'])
def upload():
    msg = ''
    if request.method == 'POST':
        hdata=request.files['households']
        tdata=request.files['transactions']
        pdata=request.files['products']
        conn = pymssql.connect(server=config['host'], user=config['user'], password=config['password'], database=config['database'])
        if hdata.filename == '' or tdata.filename == '' or pdata.filename == '' :
            msg='No Files passed'
            return render_template('upload.html', msg=msg)
        else:
            file_path = os.path.join(app.config['UPLOAD_FOLDER'],hdata.filename)
            hdata.save(file_path)
            col_names=['HSHD_NUM','L','AGE_RANGE','MARITAL','INCOME_RANGE','HOMEOWNER','HSHD_COMPOSITION','HH_SIZE','CHILDREN']
            csvData = pd.read_csv(file_path,names=col_names,header=0)
            csvData.columns = col_names
            csvData.fillna('null',inplace=True)
            tpls = [tuple(x) for x in csvData.to_numpy()]
            btch_tpls = batch(tpls,1000)
            cursor = conn.cursor()
            for i in btch_tpls:
                values = ', '.join(map(str, i))
                query='INSERT INTO households (HSHD_NUM,L,AGE_RANGE,MARITAL,INCOME_RANGE,HOMEOWNER,HSHD_COMPOSITION,HH_SIZE,CHILDREN) VALUES {}'.format(values)
                # cursor.executemany(query, tpls)
                cursor.execute(query)
                conn.commit()
            cursor.close()


            #transaction data
            file_path = os.path.join(app.config['UPLOAD_FOLDER'],tdata.filename)
            tdata.save(file_path)
            col_names=['BASKET_NUM','HSHD_NUM','PURCHASE_','PRODUCT_NUM','SPEND','UNITS','STORE_R','WEEK_NUM','YEAR']
            csvData = pd.read_csv(file_path,names=col_names,header=0)
            csvData.columns = col_names
            csvData.fillna('null',inplace=True)
            tpls = [tuple(x) for x in csvData.to_numpy()]
            btch_tpls = batch(tpls,1000)
            cursor = conn.cursor()
            for i in btch_tpls:
                values = ', '.join(map(str, i))
                query='INSERT INTO transactions (BASKET_NUM,HSHD_NUM,PURCHASE_,PRODUCT_NUM,SPEND,UNITS,STORE_R,WEEK_NUM,YEAR) VALUES {}'.format(values)
                # cursor.executemany(query, tpls)
                cursor.execute(query)
                conn.commit()
            cursor.close()

            #Products data
            file_path = os.path.join(app.config['UPLOAD_FOLDER'],pdata.filename)
            pdata.save(file_path)
            col_names=['PRODUCT_NUM','DEPARTMENT','COMMODITY','BRAND_TY','NATURAL_ORGANIC_FLAG']
            csvData = pd.read_csv(file_path,names=col_names,header=0)
            csvData.columns = col_names
            csvData.fillna('null',inplace=True)
            tpls = [tuple(x) for x in csvData.to_numpy()]
            btch_tpls = batch(tpls,1000)
            cursor = conn.cursor()
            for i in btch_tpls:
                values = ', '.join(map(str, i))
                query='INSERT INTO products (PRODUCT_NUM,DEPARTMENT,COMMODITY,BRAND_TY,NATURAL_ORGANIC_FLAG) VALUES {}'.format(values)
                # cursor.executemany(query, tpls)
                cursor.execute(query)
                conn.commit()
            cursor.close()
            msg='Sucessfully Inserted data !!!!!'
            return render_template("upload.html", msg=msg)
    else:
        msg="unable to insert data"
        return render_template("upload.html")


if __name__=="__main__":
    app.run(host="0.0.0.0",port=5050, debug=True)
