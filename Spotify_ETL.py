from datetime import datetime,timedelta
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials,SpotifyOAuth
import pandas as pd
import pytz
import psycopg2
from sqlalchemy import create_engine
from config_file import config

song_name=[]
artist_name=[]
played_at=[]
timestamp=[]
columns=['song_name','artist_name','played_at','timestamp']

def extract():
     scope="user-read-recently-played"
     now=datetime.now()
     # Converting the timestamp to UTC timezone since spotify displays the played_at in UTC timezone
     yesterday_time=datetime.now(pytz.utc)-timedelta(days=1)  
     d_in_milliseconds=int(yesterday_time.timestamp())*1000
     sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))
     results = sp.current_user_recently_played(limit=50,after=d_in_milliseconds)

     for item in results['items']:
                    song_name.append(item['track']['name'])
                    artist_name.append(item['track']['album']['artists'][0]['name'])
                    played_at.append(item['played_at'])
                    timestamp.append(item['played_at'][0:10])

     df=pd.DataFrame(columns=columns)
     song_details={'song_name':song_name,'artist_name':artist_name,'played_at':played_at,'timestamp':timestamp}
     df_song_details=pd.DataFrame(song_details)
     dataframe=pd.concat([df,df_song_details])
     print(dataframe)
     return dataframe


def transform(extracted_data):
        
        # Checking if dataframe is empty. It means you haven't listened to any songs in the past 24 hours
        if extracted_data.empty:
                print('No data is present')
                return False
        # Checks if played_at column is unique or not. We consider played_at as the primary key
        if pd.Series(extracted_data['played_at']).unique:
                pass
        else:
                raise Exception('Primary Key constraint violated')
        # Check if we have null values in the dataframe
        if extracted_data.isnull().any().any():
                raise Exception('Null values found in the data')
        
        # We need to convert the datatype of the column from object to datetime to apply dt.tz_convert in the later stage

        extracted_data['played_at']=pd.to_datetime(extracted_data['played_at']) 
        # Displays the contents of played_at in Canada Timezone
        extracted_data['played_at']=extracted_data['played_at'].dt.tz_convert(pytz.timezone("Canada/Eastern"))
        extracted_data['timestamp']=extracted_data['played_at'].dt.date
        return extracted_data
        
        
def loading(transformed_data):
        
        try:
                params=config()
                
                conn_string= f"postgresql://{params['user']}:{params['pwd']}@{params['host']}:{params['port_id']}/{params['database']}" 
                 
                # Used sqlalchemy because pandas only support sqlalchemy or sqllite3. Sqlalchemy acts as ORM
                db=create_engine(conn_string)
                conn=db.connect()
                
                with psycopg2.connect(host=params['host'], dbname=params['database'],user=params['user'],password=params['pwd'],port=params['port_id']) as conn1:
                        # Creating a cursor object. Cursor helps to execute postgres sql commands
                        with conn1.cursor() as cur: 
                                command='''CREATE TABLE IF NOT EXISTS recently_played_tracks(
                                        song_name VARCHAR(300),
                                        artist_name VARCHAR(300),
                                        played_at TIMESTAMP(300),
                                        timestamp VARCHAR(300),
                                        CONSTRAINT pk_constraint PRIMARY KEY(played_at)
                                        )'''
                                cur.execute(command)
                                conn1.commit()
                                transformed_data.to_sql('recently_played_tracks',conn,if_exists='append',index=False)

        except Exception as error:
               print(error)
                        
        
def logging(message):
     timestamp_format='%Y-%m-%d-%H:%M:%S'
     now=datetime.now()
     timestamp=now.strftime(timestamp)
     with open ('log_book','a') as f:
          f.write(timestamp + ',' + message + '/n')

logging('Extraction process started')
extracted_data=extract()
logging('Extraction process ended')
logging('Transformation process started')
transformed_data=transform(extracted_data)
logging('Transformation process ended')
logging('Loading process started')
loading(transformed_data)
logging('Loading process ended')
