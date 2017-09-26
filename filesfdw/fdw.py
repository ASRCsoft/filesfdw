"""Classes for foreign data wrappers"""

import re, datetime, glob
import pandas as pd
from multicorn import ForeignDataWrapper

class LidarCsv(ForeignDataWrapper):
    """Get lidar csv/xml files"""

    def __init__(self, options, columns):
        super(LidarCsv, self).__init__(options, columns)
        self.base = options['base']

    def execute(self, quals, columns):
        # base = '/home/will/research/asrc/data/lidar_raw/'
        files = glob.glob(self.base + '*/*/*/2*')
        dates = [ datetime.datetime.strptime(re.sub('.*/|_.*', '', file), '%Y%m%d') for file in files ]
        sites = [ re.sub(self.base + '|/.*', '', file) for file in files ]
        types = [ re.sub('.*[0-9]{8}_|\..*', '', file) for file in files ]
        df = pd.DataFrame({'site': sites, 'date': dates, 'type': types, 'path': files})
        df['path'] = df['path'].str.replace('.tmp', '', case=False)
        df.set_index(['date', 'site', 'type'], inplace=True)
        df.drop_duplicates(inplace=True)
        df2 = df.unstack('type')
        # get rid of silly multiindex junk
        df2.columns = df2.columns.levels[1]
        # make nicer names
        df2.rename(columns={'radial_wind_data': 'radial',
                            'reconstruction_wind_data': 'wind',
                            'environmental_data': 'environment',
                            'whole_radial_wind_data': 'whole',
                            'sequences': 'sequences'}, inplace=True)
        df2.where((pd.notnull(df2)), None, inplace=True)
        df2.reset_index(inplace=True)
        # add missing columns if needed
        columns = ['date', 'site', 'radial', 'wind', 'whole',
                   'scan', 'environment', 'config', 'sequences']
        for column in columns:
            if not column in df2.columns:
                df2[column] = None
        for i, row in df2.iterrows():
            yield row.to_dict()

            
class MwrCsv(ForeignDataWrapper):
    """Get microwave radiometer csv files"""

    def __init__(self, options, columns):
        super(MwrCsv, self).__init__(options, columns)
        self.base = options['base']

    def execute(self, quals, columns):
        # base = '/home/will/research/asrc/data/lidar_raw/'
        files = glob.glob(self.base + '*/*/*/2*')
        times = [ datetime.datetime.strptime(re.sub('.*/|_[a-z].*|\..*', '', file), '%Y-%m-%d_%H-%M-%S') for file in files ]
        sites = [ re.sub(self.base + '|/.*', '', file) for file in files ]
        types = [ re.sub('.*/[^a-z]*|\..*', '', file) for file in files ]
        df = pd.DataFrame({'site': sites, 'time': times, 'type': types, 'path': files})
        df['path'] = df['path'].str.replace('.tmp', '', case=False)
        df.set_index(['time', 'site', 'type'], inplace=True)
        df.drop_duplicates(inplace=True)
        df2 = df.unstack('type')
        # get rid of silly multiindex junk
        df2.columns = df2.columns.levels[1]
        df2.where((pd.notnull(df2)), None, inplace=True)
        df2.reset_index(inplace=True)
        df2['time'] = df2['time'].dt.strftime('%Y-%m-%d %X')
        # add missing columns if needed
        columns = ['time', 'site', 'lv0', 'lv1', 'lv2', 'tip', 'healthstatus']
        for column in columns:
            if not column in df2.columns:
                df2[column] = None
        for i, row in df2.iterrows():
            yield row.to_dict()

class LidarNetcdf(ForeignDataWrapper):
    """Get lidar netcdf files"""

    def __init__(self, options, columns):
        super(LidarNetcdf, self).__init__(options, columns)
        self.base = options['base']

    def execute(self, quals, columns):
        # base = '/home/will/research/asrc/data/lidar_raw/'
        files = glob.glob(self.base + '*/*/*/2*')
        dates = [ datetime.datetime.strptime(re.sub('.*/|_.*', '', file), '%Y%m%d') for file in files ]
        sites = [ re.sub(self.base + '|/.*', '', file) for file in files ]
        
        # get scans here?
        # ---
        
        df = pd.DataFrame({'site': sites, 'date': dates, 'netcdf': files})
        for i, row in df.iterrows():
            yield row.to_dict()
