############################################################
#    This queries the Billboard chart database, matches    #
#    to Million Song Dataset IDs, and generates a file     #
#    that is used for lyric database creation by           #
#    lyricGrabber.py                                       #
############################################################



import os
import re
import string
import sys
import glob
import time
import datetime
import numpy as np
try:
    import sqlite3
except ImportError:
    print 'you need sqlite3 installed to use this program'
    sys.exit(0)


####### LIST OF ARTISTS NOT IN MILLION SONG DATASET ############
skipArtist=['Geto Boys', 'The KLF', 'Marky Mark', 'Eugene Wilde', 'Glenn Frey', 'Clarence Clemons', 'Artists United Against Apartheid', 'David Foster',
            'Icehouse', 'Def Leppard', 'Buster Poindexter', 'Trixter', 'Toad The Wet Sprocket', 'Rob Thomas', 'Glee Cast','Mumford', 'Bruno Mars', 'Webbie', 'Isyss','Trent Tomlinson',
            'O-Town', 'Nick Cannon', 'Deadeye Dick', 'Smart E s', '20 Fingers', 'Everlast', 'Goodfellaz', 'The Timelords', 'Cheryl Pepsii Riley', 'Chunky A', 'Shaquille O Neal',
            'Oran  Juice  Jones', 'Secret Ties', 'Benjamin Orr', 'The Kane Gang', 'Martha Davis', 'Michael Damian', 'Diving For Pearls', '20 Fingers','Max-A-Million',
            'Shanice Wilson','Giant Steps','Boys Club','L Trimm','Tommy Conwell And The Young Rumblers','Saraya','Kevin Paige','Michael Morales','D-Mob','Christopher Max','Christopher Williams','Sharon Bryant','2nu','Timmy T.','Jon Bon Jovi','Icy Blu','Lisette Melendez','Stacy Earl','Damian Dame','Blue Train','Voyce','Natural Selection','Natural Selection','Chante Moore','N2Deep','Classic Example','AB Logic','Sound Factory','RAab','The Goodmen','Mista Grimm','Lisa Keith','Lisette Melendez','Immature','Ill Al Skratch','N II U','Martin Page','4PM','Des ree','Immature','Artie The 1 Man Party','Immature','Planet Soul','L.B.C. Crew','Terry Ellis','The Blackout Allstars','Rockell','Merril Bainbridge','The Braids','Ghost Town DJ''s','Billy Lawrence','Kimberly Scott','She Moves','WC From Westside Connection','Davina','Imani Coppola','Jo Dee Messina','Mo Thugs Family','Willie Max','SheDaisy','Jo Dee Messina','Vertical Horizon','Mikaila','Cash Money Millionaires','The Clark Family Experience','Jo Dee Messina','Steve Holy','Daniel Rodriguez','Ryan Duarte','Scotty Emerick','Rodney Atkins','Body Head Bangerz','SheDaisy','Cast Of Rent','Heartland','Rodney Atkins','Lil'' Boosie','Rodney Atkins','David Nail','Orianthi','Josh Thompson','Lee Brice','The Ready Set','Rodney Atkins','Waka Flocka Flame','Cali Swag District','The Band Perry','Neon Trees']
#For compiling artists that don't work (this was used to compile the above list)
badArtist = []


### Path to databases
dbfile = './MillionSongSubset/AdditionalFiles/track_metadata.db'
chartdb ='./chartdb.db'
#### Connect to databases
conn = sqlite3.connect(dbfile)
charconn = sqlite3.connect(chartdb)
# make cursor for queries
c = conn.cursor()
ctc = charconn.cursor()

#on chardb,  the main table name is 'songs'
TABLENAME = 'songs'



#Verify that DB connection works, display
q = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
res = ctc.execute(q)
print "Tables contained in chart database:%s" % (TABLENAME)
yo =  res.fetchall()
print yo[0][0]
print type(yo[0][0])

#Initialize counters to tally which billboard songs are/aren't found on dataset
found = 0
notfound = 0

#Files to list which songs did/didn't match dataset
SNGS = open('dataSongs.txt', 'w')
BADSNGS = open('dataSongsBad.txt', 'w')
# ORDER OF BILLBOARD'S DB RESULTS ARE =
#CHARTID | DATE | RANK | SONGNAME | ARTIST | PROMOTION | DISTRIBUTION | PEAK | WEEKSON |

SNGS.write('ChartID\tChartDate\tChartRank\tChartTitle\tChartArtist\tPromoLabel\tDistLabel\tChartPeak\tWeeksOn\tMil_trackID\tMil_title\tMil_songID\tMil_album\tMil_artistID\tMB_artistID\tMil_artit\tMil_duration\tMil_familiar\tMil_hott\tMil_year\n')
for yep in yo:

    print yep[0]
    if yep[0].find( 'Album' ) is not -1: continue
    q = "SELECT * FROM '" + yep[0] + "'"
    res = ctc.execute(q)
    charty =  res.fetchall()

    for track in charty:
        chartId = track[0]
        chartDate = track[1]
        chartRank = track[2]
        chartTitle = track[3]
        chartArtist = track[4]
        chartPromo = track[5]
        chartDist = track[6]
        chartPeak = track[7]
        chartWeeks = track[8]



        #create query  - safe variables
        artist2 = track[4]
        song = track[3].strip()
        artist = track[4].strip()

        #regex to filter out simple junk
        song = song.replace("'", "''")
        artist = artist.replace("'", "''")
        artist = re.sub(r'\([^)]*\)', '', artist)
        artist = re.sub(r'Feat.*', '', artist)
        artist = re.sub(r'\&.*', '', artist)
        artist = re.sub(r'With.*', '', artist)
        artist = re.sub(r'\*', ' ', artist)
        song =     re.sub(r'\([^)]*\)', '', song)
        if artist.strip() in skipArtist: continue


        q = "SELECT * FROM songs WHERE artist_name LIKE'%" + artist.strip() + "%' AND title LIKE '%" + song.strip()  + "%'"


        res2 = c.execute(q)
        grab =  res2.fetchone()
        print grab

        #GRAB is returned in this format ---->
        #track_id, title, song_id, release, artist_id, artist_mbid, artist_name, duration, artist_familiarity, artist_hotttnesss, year

        # If there isn't a match from basic query, run it through this set of modifications
        if grab is None:
            #A cluster of regex used to address matching problems
            if artist.find( 'line Dion' ) is not -1: artist = 'Dion'
            if artist.find( 'Motley' ) is not -1: artist = 'tley Cr'
            if artist.find( 'Maroon5' ) is not -1: artist = 'Maroon 5'
            if artist.find( 'Michael Bubl' ) is not -1: artist = 'Michael Bubl'
            if artist.find( 'Missy Misdemeanor Elliott' ) is not -1: artist = 'Missy Elliott'
            if artist.find( 'Oates' ) is not -1: artist = 'Oates'
            if artist.find( 'Kid  N Play' ) is not -1: artist = 'Play'
            if artist.find( 'Tony Toni Tone' ) is not -1: artist = 'Toni'
            if artist.find( 'Pete Townsend' ) is not -1: artist = 'Pete Townshend'
            if artist.find( 'Shakespear s Sister' ) is not -1: artist = 'Shakespears Sister'
            if artist.find( 'Thirty Seconds To Mars' ) is not -1: artist = '30 Seconds To Mars'
            if artist.find( '98 Degrees' ) is not -1: artist = '98'
            if artist.find( 'matchbox 20' ) is not -1: artist = 'matchbox twenty'
            if artist.find( 'US 3' ) is not -1: artist = 'Us3'
            if artist.find( 'Cee Lo' ) is not -1: artist = 'Cee-lo'
            if artist.find( 'Ta Mara' ) is not -1: artist = 'Tamara'
            if artist.find( 'Powersource' ) is not -1: artist = 'Power Source'
            if artist.find( 'Mellencamp' ) is not -1: artist = 'Mellencamp'
            if artist.find( 'Beyonc' ) is not -1: artist = 'Beyonc'

            artist = re.sub(r'And.*', '', artist)
            artist = re.sub(r'^The\s', '', artist)
            artist = re.sub(r'Introducing.*', '', artist)
            artist = re.sub(r'Co-.*', '', artist)
            artist = re.sub(r',.*', '', artist)
            artist = re.sub(r'\+.*', '', artist)
            artist = re.sub(r'/.*', '', artist)
            artist = re.sub(r"\sN\s", " N''", artist)
            artist = re.sub(r"\sO\s", " O'' ", artist)
            artist = re.sub(r"\ss\s", "''s ", artist)

            artist = re.sub(r"\ss$", "''s", artist)
            artist = re.sub(r"\s\s", " ", artist)

            song = re.sub(r"\sll\s", "''ll ", song)
            song = re.sub(r"\sO\s", " O'' ", song)
            song = re.sub(r"\st", "''t ", song)
            song = re.sub(r"\ss\s", "''s ", song)
            song = re.sub(r"\sm\s", "''m ", song)
            song = re.sub(r"\ss$", "''s", song)
            song = re.sub(r"\s\s", " ", song)
            song = re.sub(r"^C\s", "C''", song)

            artist = artist.replace('.','').replace('?','').replace('/','').replace(',','').replace('!','').strip()
            if artist.find( 'Mr Cheeks' ) is not -1: artist = 'Mr.Cheeks'
            if artist.find( 'Doctor Dre' ) is not -1: artist = 'Dr. Dre'

            #### Because artist matching was thorough, 'fuzzy' matching the first 5 characters of a song actually yeilds better results
            if len(song) > 5: minisong = song[:5]
            if minisong[4] == "'": minisong = song[:6]

            # Attempt a match!
            q = "SELECT * FROM songs WHERE artist_name LIKE'%" + artist+ "%' AND title LIKE '%" + minisong  + "%'"
            res2 = c.execute(q)
            grab =  res2.fetchone()

        #tally non-matches

        if grab is None:
            notfound = notfound + 1
            badart =  artist + '\t|| ' + song
            BADSNGS.write(badart)
            print artist + "\t" + song
            if badart not in badArtist:
                badArtist.append(badart)

        #tally matches, and create text file of matches and all possible variables, will get put to database
        #in lyric grabber script
        else:
            found = found + 1
            songline = '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (track[0], track[1],track[2],track[3],track[4],track[5],track[6],track[7],track[8],grab[0],grab[1],grab[2],grab[3],grab[4],grab[5],grab[6],grab[7],grab[8],grab[9],grab[10])
            songline = songline.encode('utf-8')
            print songline
            SNGS.write(songline)

##### Uncomment to display match counts
##### print 'FOUND = ' + `found` + '\tNOT FOUND = ' + `notfound`


c.close()
conn.close()

