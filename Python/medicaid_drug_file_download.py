import requests, pandas as pd, datetime as dt, sqlalchemy as sql

# Pull data from medicaid.gov API
response = requests.get('https://data.medicaid.gov/api/1/metastore/schemas/dataset/items/0ad65fe5-3ad3-5d79-a3f9-7893ded7963a')
url = response.json()['distribution'][0]['downloadURL']

# The columns we want
src_types = {'Labeler Name': str
            ,'NDC': str
            ,'Drug Category': str
            ,'Termination Date': str
            ,'Unit Type': str
            ,'Units Per Pkg Size': float
            ,'FDA Approval Date': str
            ,'Market Date': str
            ,'FDA Therapeutic Equivalence Code': str
            ,'FDA Product Name': str
            ,'Clotting Factor Indicator': str
            ,'Pediatric Indicator': str
            ,'Package Size Intro Date': str
            ,'Purchased Product Date': str
            ,'COD Status': str
            ,'FDA Application Number': str
            ,'Reactivation Date': str
            ,'Line Extension Drug Indicator': str
            }
df = pd.read_csv(url, dtype = src_types)

# Get the most recent year and most recent quarter of that year so we're only pulling the latest data
year = df['Year'].max()
year_filter = (df['Year'] == year)
quarter = df[year_filter]['Quarter'].max()
data_filter = (df['Year'] == year) & (df['Quarter'] == quarter)
data = df[data_filter]

# Rename columns
cols = {'NDC'                             : 'ndc'
       ,'Labeler Name'                    : 'labelerName'
       ,'Drug Category'                   : 'drugCategory'
       ,'Termination Date'                : 'terminationDate'
       ,'Unit Type'                       : 'unitType'
       ,'Units Per Pkg Size'              : 'unitsPerPkgSize'
       ,'FDA Approval Date'               : 'fdaApprovalDate'
       ,'Market Date'                     : 'marketDate'
       ,'FDA Therapeutic Equivalence Code': 'fdaTherEquivCode'
       ,'FDA Product Name'                : 'fdaProductName'
       ,'Clotting Factor Indicator'       : 'clottingFactorIndicator'
       ,'Pediatric Indicator'             : 'pediatricIndicator'
       ,'Package Size Intro Date'         : 'packageSizeIntroDate'
       ,'Purchased Product Date'          : 'purchasedProductDate'
       ,'COD Status'                      : 'codStatus'
       ,'FDA Application Number'          : 'fdaApplNo'
       ,'Reactivation Date'               : 'reactivationDate'
       ,'Line Extension Drug Indicator'   : 'lineExtDrugIndicator'
       }
data = data[cols.keys()].rename(columns = cols)

# Data transformations
data['unitsPerPkgSize']      = data['unitsPerPkgSize'] / 1000 # Add decimal point per data definition
data['terminationDate']      = pd.to_datetime(data['terminationDate'])
data['fdaApprovalDate']      = pd.to_datetime(data['fdaApprovalDate'])
data['marketDate']           = pd.to_datetime(data['marketDate'])
data['packageSizeIntroDate'] = pd.to_datetime(data['packageSizeIntroDate'])
data['purchasedProductDate'] = pd.to_datetime(data['purchasedProductDate'])
data['reactivationDate']     = pd.to_datetime(data['reactivationDate'])

# Map destination datatypes
dest_types = {'ndc'                    : sql.types.VARCHAR(length=11)
             ,'labelerName'            : sql.types.VARCHAR(length=39)
             ,'drugCategory'           : sql.types.VARCHAR(length=1)
             ,'terminationDate'        : sql.DateTime()
             ,'unitType'               : sql.types.VARCHAR(length=3)
             ,'unitsPerPkgSize'        : sql.types.NUMERIC(precision=10, scale=3)
             ,'fdaApprovalDate'        : sql.DateTime()
             ,'marketDate'             : sql.DateTime()
             ,'fdaTherEquivCode'       : sql.types.VARCHAR(length=2)
             ,'fdaProductName'         : sql.types.VARCHAR(length=70)
             ,'clottingFactorIndicator': sql.types.VARCHAR(length=1)
             ,'pediatricIndicator'     : sql.types.VARCHAR(length=1)
             ,'packageSizeIntroDate'   : sql.DateTime()
             ,'purchasedProductDate'   : sql.DateTime()
             ,'codStatus'              : sql.types.VARCHAR(length=2)
             ,'fdaApplNo'              : sql.types.VARCHAR(length=7)
             ,'reactivationDate'       : sql.DateTime()
             ,'lineExtDrugIndicator'   : sql.types.VARCHAR(1)
             ,'lastUpdateDate'         : sql.DateTime()
             }

load_time = dt.datetime.now()
data['lastUpdateDate'] = load_time

# Database connection information from Azure
server = <server>
database = <database>
user = dbutils.secrets.get(scope = <scope>, key = <key that contains username>)
pwd = dbutils.secrets.get(scope = <scope>, key = <key that contains password>)

# Database table information
schema = <desetination schema>
table = <destination table>
stage_schema = <staging schema>

# Establish SQLAlchemy Engine
uri = 'mssql+pyodbc://' + user + ':' + pwd + '@' + server + '/' + database + '?driver=ODBC+Driver+17+for+SQL+Server'
engine = sql.create_engine(uri, fast_executemany = True)

# Dump data to stage schema first. This is done in a replace manner because to_sql does not behave properly with 
# deleting the data prior to an append within a single transaction
with engine.begin() as conn:
    data.to_sql(name = table, schema = stage_schema, if_exists = 'replace', index = False, con = engine, dtype = dest_types)

# Copy data from stage to cms within one transaction. Any issues loading into the cms table will cause a rollback of the delete,
# preserving transactional behavior
with engine.begin() as conn:
    tbl = schema + '.' + table
    stg_tbl = stage_schema + '.' + table
    conn.execute('TRUNCATE TABLE ' + tbl)
    sql_cmd = '''INSERT INTO {tbl}
                 (
                     ndc
                   , labelerName
                   , drugCategory
                   , terminationDate
                   , unitTYpe
                   , unitsPerPkgSize
                   , fdaApprovalDate
                   , marketDate
                   , fdaTherEquivCode
                   , fdaProductName
                   , clottingFactorIndicator
                   , pediatricIndicator
                   , packageSizeIntroDate
                   , purchasedProductDate
                   , codStatus
                   , fdaApplNo
                   , reactivationDate
                   , lineExtDrugIndicator
                   , lastUpdateDate
                 )
                 SELECT ndc
                      , labelerName
                      , drugCategory
                      , terminationDate
                      , unitTYpe
                      , unitsPerPkgSize
                      , fdaApprovalDate
                      , marketDate
                      , fdaTherEquivCode
                      , fdaProductName
                      , clottingFactorIndicator
                      , pediatricIndicator
                      , packageSizeIntroDate
                      , purchasedProductDate
                      , codStatus
                      , fdaApplNo
                      , reactivationDate
                      , lineExtDrugIndicator
                      , lastUpdateDate
                   FROM {stg_tbl}
              '''.format(tbl = tbl, stg_tbl = stg_tbl)
    conn.execute(sql_cmd)
