import pandas
import datetime
from pandas.tseries.offsets import DateOffset

def interval_set(start, end, freq='D', max_delta=None, use_pandas=False):
    """ Create an exhaustive range of intervals from start to end. 
        For example:
        [1/1/2011, 1/2/2011
        1/1/2011, 1/3/2011,
        ...
        1/1/2011, 12/31/2011,
        ...
        12/30/2011, 12/31/2011]

        Frequency is a Pandas frequency string, such as "10D" "Y", etc.
        max_delta is a Pandas Timedelta object. 
        See: 
            - http://pandas.pydata.org/pandas-docs/dev/timeseries.html
            - http://pandas.pydata.org/pandas-docs/dev/timedeltas.html
        
        Examples ::

            interval_set('1/1/2011', '1/1/2014') #collection of (id, start, end) with a one day freq.
            interval_set('1/1/2011', '1/1/2014', freq='10D')
            interval_set('1/1/2011', '1/1/2014', freq='D', max_delta=pandas.Timedelta(days=2)

        In the last example, intervals may only be separated by at most 2 days.
    """ 
    date_range = pandas.date_range(start, end, freq = freq)
    date_frame = pandas.DataFrame(date_range, columns=['date'])
    date_frame['dummy_cross'] = 1
    intervals = pandas.merge(date_frame, date_frame, on='dummy_cross', suffixes=['_start', '_end'])
    del intervals['dummy_cross']
    intervals = intervals[(intervals.date_start < intervals.date_end)]
    if max_delta:
        intervals = intervals[((intervals.date_end - intervals.date_start) <= max_delta)]
    if not use_pandas:
      intervals = intervals.to_records()
      ints = []
      for (id, start, end) in intervals:
        start = start.astype('M8[D]').astype(datetime.datetime)
        end = end.astype('M8[D]').astype(datetime.datetime)
        ints.append((id, start, end))
      intervals = ints
    return intervals
   
