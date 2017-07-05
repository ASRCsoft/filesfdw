# filesfdw
A postgres foreign data wrapper to help keep track of data files

## Installation
Install with

```bash
pip install git+https://github.com/ASRCsoft/filesfdw.git
```

Make sure to install the package to the python installation used by postgres (should be the operating system's python 2).

## Setup
After installing, create the multicorn extension in the postgres database:

```sql
CREATE EXTENSION multicorn;
```

Restart postgres (from the terminal):

```bash
sudo service postgresql restart
```

In the database, create the foreign data servers and tables. Option `base` is the folder holding the data files:

```sql
# lidar csv files
CREATE SERVER lidar_csv_srv foreign data wrapper multicorn options(wrapper 'filesfdw.fdw.LidarCsv');
CREATE FOREIGN TABLE lidar_csv (date date, site text, radial text, scan text, environment text, config text, wind text, whole text) server lidar_csv_srv options(base '/farm1/mesonet/data/lidar_raw/');

# microwave radiometer csv files
CREATE SERVER mwr_csv_srv foreign data wrapper multicorn options(wrapper 'filesfdw.fdw.MwrCsv');
CREATE FOREIGN TABLE mwr_csv (time timestamp, site text, lv0 text, lv1 text, lv2 text, tip text, healthstatus text) server mwr_csv_srv options(base '/farm1/mesonet/data/mwr_raw/');

# lidar netcdf files
CREATE SERVER lidar_nc_srv foreign data wrapper multicorn options(wrapper 'filesfdw.fdw.LidarNetcdf');
CREATE FOREIGN TABLE lidar_netcdf (date date, site text, netcdf text) server lidar_nc_srv options(base '/farm1/mesonet/data/lidar_netcdf/');
```

## Usage
The foreign data tables `lidar_csv`, `mwr_csv`, and `lidar_netcdf` can now be queried just like any other postgres database. When a query is run, postgres runs a python script that finds the relevant data files and extracts some information from them (date/time, site, type of data file). The results of that script are then sent back to postgres and treated like a regular postgres table.
