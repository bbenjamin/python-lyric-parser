##############################################################
#   Takes Millon Song Dataset IDs of billboard chart         #
#    songs, matches it to thir supplementary lyric database, #
#    and processes it in different ways to create several    #
#    tables with different statistics                        #
#                                                            #
##############################################################


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


#######################################
#Class to allow multidimensional Dicts#
#######################################

class Ddict(dict):
    def __init__(self, default=None):
        self.default = default

    def __getitem__(self, key):
        if not self.has_key(key):
            self[key] = self.default()
        return dict.__getitem__(self, key)

#Swear list usualy includes obscene terms for search, not present to allow a more pleasant source code experience
swearList = ['ass','cock','dick', 'fuck, fuckin','fucker','hell', 'motherfuck', 'motherfuckin', 'slut' ,'shit', 'sex','sexi']
genres = ['funk','rnb','electro','disco', 'rock', 'hard rock','blues', 'blues rock', 'classic rock', 'electro disco', 'pop','soundtrack', 'soul', 'new wave', 'pop rock', 'funky rnb','instrumental', 'dance', 'singer-songwriter', 'singer songwriter','reggae','freestyle', 'alternative', 'post-punk', 'metal', 'heavy-metal','heavy metal', 'male vocalists', 'male vocalist','power ballad', 'power ballads', 'techno','hair metal', 'glam metal', 'synth pop', 'synthpop', 'soft rock', 'boogie','political rock', 'latin', 'love songs', 'country', 'slow jams','eighties', 'diva', 'alternative rock', 'motown', 'jazz', 'southern rock''rhythm and blues', 'rap', 'hip-hop', 'hip hop', 'quiet storm', 'latin','latin jazz','ballad', 'new age', 'electronica', 'new romantic','new jack swing', 'swing', 'oldies', 'easy listening','folk', 'folk rock', 'adult contemporary', 'power pop', 'smooth jazz','trip-hop', 'trip hop', 'acoustic', 'thrash metal', 'indie rock', 'indie','eurodance', 'gangsta', 'gangsta rap', 'r and b', 'grunge', 'christian','ska']


dbfile = './mxm_dataset.db'
ratiodb = './lyric_ratio.db'

### Connect to Million song
conn = sqlite3.connect(dbfile)
c = conn.cursor()


conn_ratio = sqlite3.connect(ratiodb, isolation_level=None)
c_ratio = conn_ratio.cursor()

##########################################
#Uncomment when you want to reset tables #
##########################################
try:
    #c_ratio.execute('DROP TABLE wordRatio')
    #c_ratio.execute('DROP TABLE wordYear')
    #c_ratio.execute('DROP TABLE wordPercent')
    #c_ratio.execute('DROP TABLE songCount')
    #c_ratio.execute('DROP TABLE wordChanges')
    print 'attempted drops'
except:
    print "no old tables to drop"



#############################################
#Uncomment when you want to recreate tables #
#############################################

#c_ratio.execute('CREATE TABLE songCount(id INTEGER PRIMARY KEY,year TEXT, word TEXT, count TEXT, percent TEXT, totalSongsThatYear TEXT)')
#c_ratio.execute('CREATE TABLE wordRatio(id INTEGER PRIMARY KEY, year TEXT, word TEXT, count TEXT)')
#c_ratio.execute('CREATE TABLE wordYear(id INTEGER PRIMARY KEY, year TEXT, totalWords TEXT)')
#c_ratio.execute('CREATE TABLE wordPercent(id INTEGER PRIMARY KEY, year TEXT, word TEXT, percent TEXT)')
#c_ratio.execute('CREATE TABLE wordChanges(id INTEGER PRIMARY KEY, interval TEXT, word TEXT, change TEXT)')
#c_ratio.execute('CREATE TABLE genreByTags(id INTEGER PRIMARY KEY, artist TEXT, song TEXT, year TEXT, genre TEXT)')
#c_ratio.execute('CREATE TABLE genreFrequency(id INTEGER PRIMARY KEY, year TEXT, genre TEXT, occurs TEXT, ratio TEXT)')
#c_ratio.execute('CREATE TABLE totalWordsByYear(id INTEGER PRIMARY KEY, year TEXT, total TEXT)')


#############################################
#Code to populate word percent table        #
#############################################

q = "SELECT wordRatio.year, wordRatio.word, wordRatio.count, wordYear.totalWords FROM wordRatio, wordYear WHERE wordRatio.year = wordYear.year"
git = c_ratio.execute(q)
grab =  git.fetchall()
for gr in grab:
    Wyear = gr[0]
    Wword = gr[1]
    Wcount = gr[2]
    Wtotal = gr[3]
    Wpercent = float(Wcount) / float(Wtotal)
    Wpercent = '{0:.20f}'.format(float(Wpercent))
    #print Wpercent

    ####### UNCOMMENT TO WRITE TO wordPercent table
    #qu = "INSERT INTO wordPercent (year, word, percent) VALUES('%s','%s','%s')"  % (Wyear, Wword, Wpercent)
    #c_ratio.execute(qu)

newWords = []   #to include when a new word appears

##Create multidimensional dict, to carry percentage of word occurences by year
yearWords = Ddict(dict)

################################################################
#Get percentages for word occurrence , sort by year and word   #
################################################################
q = "SELECT year, word, percent FROM wordPercent"
git = c_ratio.execute(q)
grab =  git.fetchall()
#### DUDE
for g in grab:

    yr = g[0]
    wrd = g[1]
    percent = g[2]
    percent = '{0:.20f}'.format(float(percent))

    if int(yr) < 1985: continue

    yearWords[yr][wrd] = percent

####################################################################################################
# Create files, and iterate through wordpercent to document new words and top words from each year##
####################################################################################################

NEW = open('newWordsByYear.txt', 'w')
NEW.write("year\tword\n")
TOP = open('top20WordsByYear.txt', 'w')
TOP.write("pos\tword\tyear\tpercent used\n")
for key, value in sorted(yearWords.iteritems()):
    counter = 0
    nextyear = int(key) + 1
    if nextyear > 2010: continue
    # c_ratio.execute('CREATE TABLE wordChqnges(id INTEGER PRIMARY KEY, interval TEXT, word TEXT, change TEXT)')
    for wrd, percent in sorted(value.iteritems(), key=lambda (k,v): (v,k), reverse=True):
        preNewWords = []
        timeSpan = key + '-' + `nextyear`
        preNewWords.append(wrd)
        if wrd in yearWords[key]:
            if wrd in yearWords[`nextyear`]:
                newAmount = yearWords[`nextyear`][wrd]
                change =  float(newAmount) - float(percent)
                change = '{0:.20f}'.format(float(change))
                wrd = wrd.replace("'","''")


                ######### UNCOMMENT MULTILINE TO RE-POPULATE wordChanges table

                '''  #START COMMENT
                try:
                    qu = "INSERT INTO wordChanges(interval, word, change) VALUES('%s','%s','%s')"  % (timeSpan, wrd, change)
                    c_ratio.execute(qu)
                    print "%s %s %s" % (timeSpan, wrd, change)
                    print "DUH"
                except:
                    print"unicode bummer"
                ''' # END COMMENT


    ### Iterate through yearWords to get top words by year
    count = 1
    print " TOP WORDS FOR %s" % (key)
    for wrd, percent in sorted(yearWords[key].iteritems(), key=lambda (k,v): (v,k), reverse=True):
        if count > 20: break
        wrd = wrd.encode('UTF-8')
        percent = float(percent)

        #only document words over 3 characters long
        '''
        if len(wrd) > 3:

            try:
                str = "%s\t%s\t%s\t%.16f\n" % (count, wrd,  key, percent)
                print str
                TOP.write(str)
                count = count + 1
            except:
                print "unicode print problem"

        '''
        counter = counter + 1
        #look for new words by year, write new ones to file
        #Isn't working properly right now....
        '''
        if key != '1985':
            lastyear = int(key) - 1
            if wrd not in newWords:
                if len(wrd) < 4: continue
                str = "%s\t%s\n" % (key, wrd)
                preNewWords.append(wrd)
                str = str.encode('UTF-8')
                NEW.write(str)
        newWords.append(preNewWords)
        '''







### Get genre information
q = "SELECT year, genre FROM genreByTags"
git = c_ratio.execute(q)
grub =  git.fetchall()


## SORT GENRES BY YEAR ###
genreDict = Ddict(dict)
swearByYear = Ddict(dict)
swearSongsByYear = Ddict(dict)
totalGenre = {}
for gb in grub:

    year = gb[0]
    genre = gb[1]
    if year is None: continue
    if year[0] == '0': continue
    if year < 1985: continue
    #if year > 2010: continue
    #print year
    if year not in totalGenre:
        totalGenre[year] = 1
    else:
        totalGenre[year] = totalGenre[year] + 1

    if year not in genreDict:
        genreDict[year][genre] = 1
    else:
        if genre not in genreDict[year]:
            genreDict[year][genre] = 1
        else:
            genreDict[year][genre] = genreDict[year][genre] + 1
#print genreDict
GENRE = open('genreByYear.txt', 'w')
GENRE.write("year\tgenre\tnumber of songs tagged\tpercentage of total tags\n")


### Get, and output, genre frequency by year
for year, genre in genreDict.iteritems():

    for g in genre:
        percent = float(genre[g]) / float(totalGenre[year])
        filestr = "%s\t%s\t%s\t%s\n" % (year, g, genre[g], percent)
        GENRE.write(filestr)


##########################################################################
#Pull Million song IDs from billboard matching to query the lyric dataset#
##########################################################################
trackData = open('ChartAndDataset.txt', 'r')
tags = open('singleTag.txt', 'r')
header = trackData.readline()
popularWords = {}
longWords = {}
longerWords = {}
swears = {}
filthDict = {}
wordsPerYear = {}
wordSongYear = Ddict(dict)
totalSongYear = Ddict(dict)



tracksThatYear = {}
for line in trackData:
    parts = line.split('\t')
    chartID = parts[0]
    chartDate = parts[1]
    chartYear = chartDate[:4]
    chartPosition = parts[2]
    chartTitle = parts[3]
    safeTitle = chartTitle.replace("'","''")
    chartArtistName = parts[4]
    safeArtist = chartArtistName.replace("'","''")
    promo = parts[5]
    safe5 = promo.replace("'","''")
    distributor = parts[6]
    safe6 = distributor.replace("'","''")
    chartPeak = parts[7]
    chartWeeks = parts[8]
    datasetTrackID = parts[9]
    datasetTitle = parts[10]
    safe10 = datasetTitle.replace("'","''")
    datasetSongID = parts[11]
    datasetAlbum = parts[12]
    safe12 = datasetAlbum.replace("'","''")
    datasetArtistID = parts[13]
    mbrainsArtistID = parts[14]
    datasetArtistName = parts[15]
    safe15 = datasetArtistName.replace("'","''")
    datasetDuration = parts[16]
    datasetFamiliar = parts[17]
    datasetHott = parts[18]
    datasetYear = parts[19][:4]

    ###### UNCOMMENT TO ADD YEAR VALUES TO GENRE TABLE
    #qt = "UPDATE genreByTags SET year = '%s' WHERE artist='%s' AND song='%s'" % (datasetYear, safeArtist, safeTitle)
    #print qt
    #c_ratio.execute(qt)

    ##### UNCOMMENT TO WRITE EVERY SONGS DATA TO A HUGE TABLE
    #qr = "INSERT INTO chartData(chartID, chartDate, chartPosition , chartTitle, chartArtistName, promo, distributor, chartPeak, chartWeeks, datasetTrackID, datasetTitle, datasetSongID, datasetAlbum, datasetArtistID, mbrainsArtistID, datasetArtistName, datasetDuration, datasetFamiliar, datasetHott, datasetYear) VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (parts[0], parts[1],parts[2],safeTitle,safeArtist,safe5,safe6,parts[7],parts[8],parts[9],safe10,parts[11],safe12,parts[13],parts[14],safe15,parts[16],parts[17],parts[18],datasetYear)
    #print qr
    #c_ratio.execute(qr)

    ####### UNCOMMENT MULTILINE TO PARSE TAGS TXT FILE AND WRITE TO DB TABLE #######
    '''  #START MULTILINE COMMENT
    for line in tags:
        try:
            (song, artist, tags) = line.split('\t')
            song = song.replace("'","''")
            artist = artist.replace("'","''")
            #print song + artist + tags
            tagList = tags.split(',')
            for t in tagList:
                if t in genres:
                    q = "INSERT into genreByTags(artist, song, genre) VALUES('%s', '%s', '%s')" % (artist, song, t)
                    print q
                    c_ratio.execute(q)


        except:
            break
    ''' #END MULTILINE COMMENT

   ###### GET LYRICS FOR THIS SONG, PASS INTO DIFFERENT DICTIONARIES FOR PROCESSING
    q = "SELECT * FROM lyrics WHERE track_id ='%s'" % (datasetTrackID)
    res2 = c.execute(q)
    grab =  res2.fetchall()
    topCount = 0
    topWord = ''
    if not grab: continue
    #print grab
    filth = 0
    if chartYear not in tracksThatYear:
        tracksThatYear[chartYear] = 1
    else:
        tracksThatYear[chartYear] = tracksThatYear[chartYear] + 1

    for g in grab:
        word = g[2]
        count = g[3]
        word = word.strip()

        if chartYear not in totalSongYear:
            totalSongYear[chartYear][word] = count
        else:
            if word not in wordSongYear[chartYear]:
                totalSongYear[chartYear][word] = count
            else:
                totalSongYear[chartYear][word] = count + totalSongYear[chartYear][word]


        if chartYear not in wordSongYear:
            wordSongYear[chartYear][word] = 1
        else:
            if word not in wordSongYear[chartYear]:
                wordSongYear[chartYear][word] = 1
            else:
                wordSongYear[chartYear][word] = 1 + wordSongYear[chartYear][word]
    #put total words in
        if word in swearList:

            if chartYear not in swearByYear:
                swearByYear[chartYear][word] = count
                swearSongsByYear[chartYear][word] = 1
            else:
                if word not in swearByYear[chartYear]:
                    swearByYear[chartYear][word] = count
                    swearSongsByYear[chartYear][word] = 1
                else:
                    swearByYear[chartYear][word] = count + swearByYear[chartYear][word]
                    swearSongsByYear[chartYear][word] = 1 + swearSongsByYear[chartYear][word]




            if word not in swears:
                swears[word] = count
            else:
                swears[word] = count + swears[word]
            if datasetArtistName not in filthDict:
                filthDict[datasetArtistName] = count
            else:
                filthDict[datasetArtistName] = count + filthDict[datasetArtistName]
    #get total words per year

        if datasetYear not in wordsPerYear:
            wordsPerYear[datasetYear] = count
        else:
            wordsPerYear[datasetYear] = count + wordsPerYear[datasetYear]



        if len(word) > 3:
            if count > topCount:
                topCount = count
                topWord = word
                # print "COUNT %s %s" % (count, word)
            if word not in popularWords:
                popularWords[word] = count
            else:
                popularWords[word] = popularWords[word] + count
        if len(word) > 4:
            if word not in longWords:
                longWords[word] = count
            else:
                longWords[word] = longWords[word] + count
        if len(word) > 5:
            if word not in longerWords:
                longerWords[word] = count
            else:
                longerWords[word] = longerWords[word] + count

TS = open('totalSwearsByYear.txt', 'w')
TS.write("Year\tword\ttimesoccured\n")
for year in swearByYear:
    print year
    for word in swearByYear[year]:
        print "\t%s\t%s" % (word, swearByYear[year][word])
        str = "%s\t%s\t%s\n" % (year, word, swearByYear[year][word])
        TS.write(str)

SWEARSONG = open('songsWithSwearsByYear.txt', 'w')
SWEARSONG.write("Year\tword\tsongs that year\n")
for year in swearSongsByYear:
    print year
    for word in swearSongsByYear[year]:
        print "\t%s\t%s" % (word, swearSongsByYear[year][word])
        str = "%s\t%s\t%s\n" % (year, word, swearSongsByYear[year][word])
        SWEARSONG.write(str)

for year in totalSongYear:
    for word in totalSongYear[year]:
        cnt = totalSongYear[year][word]

         ######### UNCOMMENT TO WRITE TO WORD RATIO TABLE ##########
        #qu = "INSERT INTO wordRatio (year, word, count) VALUES('%s','%s','%s')"  % (year, word, cnt)
        #c_ratio.execute(qu)

for year in tracksThatYear:
    print "%s  %s" % (year,  tracksThatYear[year])

for year in wordSongYear:
    print year
    c = 0
    for word in wordSongYear[year]:
        numSongs = wordSongYear[year][word]
        totalSongs = tracksThatYear[year]
        ratio = float(numSongs) / float(totalSongs)
        ratio = ratio * 100

        ########### UNCOMMENT TO WRITE TO songCount
        #qu = "INSERT INTO songCount (year, word, count, percent, totalSongsThatYear) VALUES('%s','%s','%s','%s','%s')"  % (year, word, wordSongYear[year][word], ratio,totalSongs)
        #c_ratio.execute(qu)

        c = c + 1


######  DETERMINE CHANGE IN POPULARITY OF WORDS BASED ON YEARS #########
trendWords = Ddict(dict)
q = "SELECT interval, word, change FROM wordChanges"
git = c_ratio.execute(q)
grab =  git.fetchall()
for g in grab:
    intrvl = g[0]
    wrd = g[1]
    chng = g[2]
    trendWords[intrvl][wrd] = chng



########Create files to write biggest gainers and losers for words

WINNER = open('top20WordGainsByYear.txt', 'w')
WINNER.write("interval\tword\tincrease\n")
LOSER = open('top20WordDropsByYear.txt', 'w')
LOSER.write("interval\tword\tdecrease\n")
for span, word in trendWords.iteritems():
    #print span
    #print word
    #print type(word)
    print "\n#############Top Gainers For %s##############" % (span)
    yr1 = span[:4]
    yr2 = span[-4:]
    print "##%s:%s words %s:%s words\n  " % (yr1, wordsPerYear[yr1], yr2, wordsPerYear[yr2])
    count = 0

    for wrd, scor in sorted(word.iteritems(), key=lambda (k,v): (v,k), reverse=True):
        if count > 20: break
        if len(wrd) < 4:continue
        #print 'Gainer=' + wrd + '\t' + scor
        wrt = "%s\t%s\t%s\n" % (span, wrd,scor)
        wrt = wrt.encode('UTF-8')
        WINNER.write(wrt)
        count = count + 1

    print "\n#############Biggest Losers For %s##############" % (span)
    yr1 = span[:4]
    yr2 = span[-4:]
    print "##%s:%s words %s:%s words\n  " % (yr1, wordsPerYear[yr1], yr2, wordsPerYear[yr2])
    count = 0
    for wrd, scor in sorted(word.iteritems(), key=lambda (k,v): (v,k)):
        if count > 20: break
        if len(wrd) < 4:continue
        wrt = "%s\t%s\t%s\n" % (span, wrd,scor)
        wrt = wrt.encode('UTF-8')
        LOSER.write(wrt)
        count = count + 1


    try:
        #print word
        result = "Artist: %s Song: %s Most used word over 3 letters long: %s" % (datasetArtistName, datasetTitle, topWord)

        result = result.encode('utf-8', 'replace')
        print result
    except:
        print "unicode skip"

################ UNCOMMENT TO WRITE TOTAL WORDS PER YEAR TO TABLE
''' # START COMMENT
for key, value in sorted(wordsPerYear.iteritems()):
    qu = "INSERT INTO wordYear (year, totalWords) VALUES('%s','%s')"  % (key, value)
    c_ratio.execute(qu)
''' # END COMMENT


###### SOME DEMONSTRATION LOOPS
stop = 0
print "MOST POPULAR WORDS (over 3 letters long) IN THE BILLBOARD CHARTS 1980-Present!"
for key, value in sorted(popularWords.iteritems(), key=lambda (k,v): (v,k), reverse=True):
    if stop > 15: break
    try:
        print "%s: %s" % (key, value)
        stop = stop + 1
    except:
        print "unicode skip"
stop = 0
print "MOST POPULAR WORDS (over 4 letters long) IN THE BILLBOARD CHARTS 1980-Present!"
for key, value in sorted(longWords.iteritems(), key=lambda (k,v): (v,k), reverse=True):
    if stop > 15: break
    try:
        print "%s: %s" % (key, value)
        stop = stop + 1
    except:
        print "unicode skip"

stop = 0
print "MOST POPULAR WORDS (over 5 letters long) IN THE BILLBOARD CHARTS 1980-Present!"
for key, value in sorted(longerWords.iteritems(), key=lambda (k,v): (v,k), reverse=True):
    if stop > 15: break
    try:
        print "%s: %s" % (key, value)
        stop = stop + 1
    except:
        print "unicode skip"

print "HOW MANY SWEAR WORDS IN THE BILLBOARD TOP 100?"
for key, value in swears.iteritems():
    print "%s:\t%s" % (key, value)


print "TOP TEN FILTHMONGERS"
stop = 0
#for key, value in sorted(filthDict.iteritems(), key=lambda (k,v): (v,k), reverse=True):
for key, value in sorted(filthDict.iteritems(), key=lambda (k,v): (v,k), reverse=True):
    if stop > 9: break
    try:
        print "%s: %s Swears!    " % (key, value)
        stop = stop + 1
    except:
        print "unicode stuff"
    if stop > 9: break
    try:
        print "%s: %s Words   " % (key, value)
        stop = stop + 1
    except:
        print "unicode skip"


