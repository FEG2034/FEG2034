import pandas
import numpy

#header for export table-------------------------------------------------------

header_file = pandas.read_excel("https://www.gdeltproject.org/data/lookups/CSV.header.fieldids.xlsx")
head_of_export = list(header_file.axes[1])
head_of_export.insert(39, 'Actor1Geo_ADM2Code')
head_of_export.insert(47, 'Actor2Geo_ADM2Code')
head_of_export.insert(55, 'ActionGeo_ADM2Code')

dtype_of_export = {}

for col in head_of_export:
    dtype_of_export[str(col)] = str
for col in ('GLOBALEVENTID','SQLDATE','MonthYear','IsRootEvent','QuadClass','NumMentions','NumSources','NumArticles',\
            'Actor1Geo_Type','Actor2Geo_Type','ActionGeo_Type','DATEADDED'): #<----These Date are 'int'
    dtype_of_export[col] = int
for col in ('FractionDate','GoldsteinScale','AvgTone'):
    dtype_of_export[col] = float

#head of mention table---------------------------------------------------------

head_of_mention = ['GLOBALEVENTID','EventTimeDate','MentionTimeDate','MentionType',\
                   'MentionSourceName','MentionIdentifier','SentenceID',\
                   'Actor1CharOffset','Actor2CharOffset','ActionCharOffset',\
                   'InRawText','Confidence','MentionDocLen','MentionDocTone',\
                   'MentionDocTranslationInfo','Extras']

dtype_of_mention = {}

for col in head_of_mention:
    dtype_of_mention[col] = int
for col in ('MentionDocTranslationInfo','MentionSourceName','MentionIdentifier','Extras'):
    dtype_of_mention[col] = str
for col in ('EventTimeDate','MentionTimeDate','MentionDocTone'): #<----These Date are 'float'
    dtype_of_mention[col] = float

#import table and code---------------------------------------------------------
export = pandas.read_csv("http://data.gdeltproject.org/gdeltv2/20150218230000.export.CSV.zip", \
                         compression='zip', sep='\t', lineterminator='\n', nrows=50, dtype=dtype_of_export, names=head_of_export)
#export.to_csv(path_or_buf="/Users/feg2034/GDELT - 20150218230000.export.csv")

mention = pandas.read_csv("http://data.gdeltproject.org/gdeltv2/20150218230000.mentions.CSV.zip", \
                          compression='zip', sep='\t', lineterminator='\n', nrows=50, dtype=dtype_of_mention, names=head_of_mention)

CountryCode = pandas.read_csv("https://www.gdeltproject.org/data/lookups/CAMEO.country.txt", sep='\t', lineterminator='\n')
Geo_CountryCode = pandas.read_csv("https://www.gdeltproject.org/data/lookups/FIPS.country.txt", sep='\t', lineterminator='\n', names=['CODE','LABEL'])
TypeCode = pandas.read_csv("https://www.gdeltproject.org/data/lookups/CAMEO.type.txt", sep='\t', lineterminator='\n')
KnownGroupCode = pandas.read_csv("https://www.gdeltproject.org/data/lookups/CAMEO.knowngroup.txt", sep='\t', lineterminator='\n')
EthnicCode = pandas.read_csv("https://www.gdeltproject.org/data/lookups/CAMEO.ethnic.txt", sep='\t', lineterminator='\n')
ReligionCode = pandas.read_csv("https://www.gdeltproject.org/data/lookups/CAMEO.religion.txt", sep='\t', lineterminator='\n')
EventCode = pandas.read_csv("https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt", sep='\t', lineterminator='\n', dtype=str)
GoldsteinScaleCode = pandas.read_csv("https://www.gdeltproject.org/data/lookups/CAMEO.goldsteinscale.txt", sep='\t', lineterminator='\n', dtype={"CAMEOEVENTCODE":str,"GOLDSTEINSCALE":float})


