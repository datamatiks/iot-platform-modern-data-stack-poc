import duckdb

def get_conn(db=':memory:',is_shared=False):
    return duckdb.connect(database=db, read_only=is_shared)

class Database():

    def __init__(self):
        self.conn = get_conn('db/datahack.duckdb')
        self._metrics_df = self.__init_metrics_df(self.conn)
        self._attributes = self.__get_attributes()
      
    def __init_metrics_df(self, conn):
        metrics_df = conn.sql('SELECT * EXCLUDE(measured_day,measured_hour,measured_minute) FROM ingest_gauge_metrics').df()
        metrics_df = metrics_df.set_index("measured_datetime")
        metrics_df = metrics_df.astype('Float64')
        
        conn.close()
        return metrics_df

    def __get_attributes(self):
        conn = get_conn('db/datahack.duckdb')
        attrs_df = conn.sql('SELECT *  FROM gauge_sensors_metadata').df()
        conn.close()
        return attrs_df #.to_csv(index=False)

    @property
    def metrics_df(self):
        return self._metrics_df
    
    @property
    def attributes(self):
        return self._attributes

    def get_gauge_metrics(self):
        print('Get metrics')
        return self.metrics_df
     
    def get_gauge_metadata(self):
        print('Get metadata')
        return self.attributes
    
    def get_daily_avg(self):
        print('Daily Avg')
        daily_mean = self.metrics_df.resample("D").mean()
        print(daily_mean)

        return daily_mean

    def get_daily_max(self):
        print('Daily Max')
        daily_max = self.metrics_df.resample("D").max()
        print(daily_max)

        return daily_max

    def get_daily_min(self):
        print('Daily Min')
        daily_min = self.metrics_df.resample("D").min()
        print(daily_min)

        return daily_min