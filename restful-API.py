#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  1 15:30:40 2021

@author: Mohammed Al Mansour
"""


import pandas as pd
from flask import Flask
from flask import request
from flask_restx import Resource, Api
from flask_restx import fields
import sqlite3
from pandas.io import sql
import datetime
import requests
import json
from flask_restx import reqparse




app = Flask(__name__)
api = Api(app, default="TV-shows dataset methods", title="TV-shows", description="API that imports and handles tv-shows records from tv-maze. \n By Mohammed Al Mansour")

# The following is the schema of tv-show


selflink = api.model('href', {"href": fields.String})
prelink = api.model('href', {"href": fields.String})
nextlink = api.model('href', {"href": fields.String})
links = api.model('links', {'self': fields.Nested(selflink), 'previous': fields.Nested(prelink), 'next': fields.Nested(nextlink)})

rating = api.model('rating', {'average': fields.Integer})

tvshow_model = api.model('TV show', {  
   "tvmaze-id" :fields.Integer,
   "id": fields.Integer,
   "last-update": fields.String,
   "name": fields.String,
   "type": fields.String,
   "language": fields.String,
   "genres": fields.List(fields.String(example="genre")),
    "status": fields.String,
    "runtime": fields.Integer,
    "premiered": fields.String,
    "officialSite": fields.String,
    "schedule": fields.String,
     "rating": fields.Nested(rating),
      "weight": fields.Integer,
      "network": fields.String,
      "summary": fields.String,
      '_links': fields.Nested(links)
      })


#app.UseDeveloperExceptionPage();
parser = api.parser()
parser.add_argument('name', type=str)#, help='name of show to search and add', location='args')

@api.response(200, 'OK')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
@api.response(201, 'Created')
@api.param('name','tv-show name')
@api.route('/tvshows/import')
class TV1(Resource):
    @api.doc(description="Import a TV-show by its name and store it")
    # import a tv show from tvmaze and either insert into database or update if already exists
    def post(self):
        # query tvmaze for the named tv show
        
        #print(name)
        #print(type(name))
        args = parser.parse_args()

        # retrieve the query parameters
        name = args.get('name')
        
        
        url = ' http://api.tvmaze.com/search/shows?q='+str(name)
        resp = requests.get(url=url)
        data2 = resp.json()
        
        if data2 == []:
            api.abort(404, "tv-show {} doesn't exist".format(name))
        
        data2 = data2[0]['show']
        instance = data2
        
        

        # data insertion into database from json format
        now = str(datetime.datetime.now())[:-7]

        conn = sqlite3.connect('TVS.db')
        c = conn.cursor()
        now = str(datetime.datetime.now())[:-7]
        c.execute('''INSERT OR REPLACE INTO TVSHOWS (
                                          tvmazeid, 
                                          lastupdate, 
                                          name, 
                                          type, 
                                          language, 
                                          genres, 
                                          status, 
                                          runtime, 
                                          premiered, 
                                          officialSite, 
                                          schedule, 
                                          rating, 
                                          weight, 
                                          network, 
                                          summary, 
                                          links
                                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                  (instance['id'], 
                   now, 
                   instance['name'],
                   instance['type'], 
                   instance['language'], 
                   str(instance['genres']), 
                   instance['status'], 
                   instance['runtime'], 
                   instance['premiered'], 
                   instance['officialSite'], 
                   str(instance['schedule']), 
                   instance['rating']['average'], 
                   instance['weight'], 
                   str(instance['network']), 
                   instance['summary'], 
                   "http://127.0.0.1:5001/tv-shows/"+str(instance['id'])))  # change links to the required format
        conn.commit()
        print('New generated user ID is: ', c.lastrowid)
        
        ret = { 
                    "id" : c.lastrowid,  
                    "last-update": now,
                    "tvmaze-id" : instance['id'],
                    "_links": {
                        "self": {
                          "href": "http://127.0.0.1:5001/tv-shows/"+str(c.lastrowid)
                        }
                    } 
                }
        return ret, 201



@api.route('/tvshows/<int:id>')
@api.response(200, 'OK')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
@api.response(201, 'Created')
class TV2(Resource):
    @api.doc(description="Retrieve a TV-show record by its ID")
    def get(self, id):
        
        # Q2 (get)
        # retreive tv show
        
        # query a tv show with its id
        conn = sqlite3.connect('TVS.db')
        c = conn.cursor()
        c.execute('SELECT * FROM TVSHOWS WHERE id = ?', (id,))
        row = c.fetchone()
        #print(row)
        if row == None:
            api.abort(404, "tv-show {} doesn't exist".format(id))
        
        # return the correct format json
        D = {  
               "tvmaze-id" :row[1],
               "id": row[0],
               "last-update": row[2],
               "name": row[3],
               "type": row[4],
               "language": row[5],
               "genres": eval(row[6]),
                "status": row[7],
                "runtime": row[8],
                "premiered": row[9],
                "officialSite": row[10],
                "schedule": eval(row[11]),
                 "rating": {'average': row[12]},
                  "weight": row[13],
                  "network": row[14],
                  "summary": row[15],
                  "_links": {
                    "self": {
                      "href": "http://127.0.0.1:5001/tv-shows/"+str(row[0])
                    },
                    "previous": {
                      "href": "http://127.0.0.1:5001/tv-shows/"+str(row[0]-1)
                    },
                    "next": {
                      "href": "http://127.0.0.1:5001/tv-shows/"+str(row[0]+1)
                    }
                  } 
            }



        return D, 200
    
    @api.doc(description="Delete a TV-show record by its ID")
    def delete(self, id):
        
        # Q3 delete a tv show (delete)
        conn = sqlite3.connect('TVS.db')
        c = conn.cursor()
        
        c.execute('SELECT * FROM TVSHOWS WHERE id = ?', (id,))
        row = c.fetchone()
        #print(row)
        if row == None:
            api.abort(404, "tv-show {} doesn't exist".format(id))
        
        
        c.execute('DELETE FROM TVSHOWS WHERE id = ?', (id,))
        row = c.fetchone()
        #print(row)
        
        # return message in json
        r = { 
            "message" :"The tv show with id "+str(id)+" was removed from the database!",
            "id": id
        }

        
        return r, 200

    @api.doc(description="Update a TV-show record by its ID.\n Note: IDs and links cannot be updated")
    @api.expect(tvshow_model)
    def put(self, id):
        # check if id exists
        conn = sqlite3.connect('TVS.db')
        c = conn.cursor()
        
        c.execute('SELECT * FROM TVSHOWS WHERE id = ?', (id,))
        row = c.fetchone()
        #print(row)
        if row == None:
            api.abort(404, "tv-show {} doesn't exist".format(id))

        # get the payload and convert it to a JSON
        show = request.json

        # show ID cannot be changed
        if 'id' in show and id != row[0]:
            return {"message": "Identifier cannot be changed".format(id)}, 400
        if "tvmaze-id" in show and show["tvmaze-id"] != row[1]:
            return {"message": "Identifier cannot be changed".format(id)}, 400

        # add last updated to the json
        now = str(datetime.datetime.now())[:-7]
        show.update({'last-update': now})
        # Update the values
        for k,v in show.items():
            if k not in tvshow_model.keys():
                # unexpected column
                return {"message": "Property {} is invalid".format(k)}, 400
            if k=="tvmaze-id":
                k='tvmazeid'
            if k=="last-update":
                k='lastupdate'
            if k=="_links":
                continue
            if type(v)!=str:
                if type(v)!=int:
                    v=str(v)
            if type(v)==str:
                v = '"'+v+'"'
            #print('UPDATE TVSHOWS SET '+str(k)+' = '+str(v)+' WHERE id = '+str(id)+';')
            c.execute('UPDATE TVSHOWS SET '+str(k)+' = '+str(v)+' WHERE id = '+str(id)+';')
            row = c.fetchone()

        # df.append(book, ignore_index=True)
        return {"message": "tvshow {} has been successfully updated".format(id)}, 200


parser2 = api.parser()
parser2.add_argument("order_by", type=str)
parser2.add_argument("page", type=int)
parser2.add_argument("page_size", type=int)
parser2.add_argument("filter", type=str)


@api.response(200, 'OK')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
@api.response(201, 'Created')
@api.param('order_by','comma-separated')
@api.param('page','comma-separated')
@api.param('page_size','comma-separated')
@api.param('filter','comma-separated')
@api.route('/tvshows')
class TV3(Resource):
    
    @api.doc(description="Retrieve the list of available TV shows")
    def get(self):
        # Q5
        # list
        args = parser2.parse_args()

        # retrieve the query parameters
        order_by = args.get('order_by')
        page = args.get('page')
        page_size = args.get('page_size')
        filter = args.get('filter')
        conn = sqlite3.connect('TVS.db')
        c = conn.cursor()
        c.execute('SELECT * FROM TVSHOWS')
        row = c.fetchone()
        #print(row)
        if row == None:
            api.abort(404, "no tv-shows exist")
        
        # prepare query

        print(order_by)
        
        order_by2 = str(order_by)
        order_by2 = order_by.split(',')
        l = ' '
        for s in order_by2:
            if s[0]=='+':
                s = s.strip('+')
                if s=='rating-average':
                    s='rating'
                l=l+s+' ASC,'
            if s[0]=='-':
                s = s.strip('-')
                if s=='rating-average':
                    s='rating'
                l=l+s+' DESC,'
        l=l.strip(',')
        
        conn = sqlite3.connect('TVS.db')
        c = conn.cursor()
        c.execute('''SELECT
                       '''+str(filter)+'''
                        FROM
                           TVSHOWS
                        ORDER BY
                        '''+str(l)+''' ;''')
        row = c.fetchall()
        if len(row) > page_size:
            row = row[page_size*(page-1):page_size*(page)-1]
        # create response dictionary json format
        flist = str(filter).split(',')
        for i in range(len(flist)):
            flist[i]=flist[i].strip(' ')
        d = dict()
        tvshows = []
        for i in range(len(row)):
            for j in range(len(flist)):
                if flist[j]=='rating':
                    d.update({'rating': {'average': row[i][j]}})
                elif flist[j] in ['genres', 'schedule', 'network']:
                    d.update({flist[j]:eval(row[i][j])})
                else:
                    d.update({flist[j]:row[i][j]})
            tvshows.append(d)
            d = dict()
        
        D = {
            "page": page,
            "page-size": page_size,
            "tv-shows": tvshows,
            "_links": {
                "self": {
                  "href": "http://127.0.0.1:5001/tv-shows?order_by="+str(order_by)+"&page="+str(page)+"&page_size="+str(page_size)+"&filter="+str(filter)
                },
                "next": {
                  "href": "http://127.0.0.1:5001/tv-shows?order_by="+str(order_by)+"&page="+str(page+1)+"&page_size="+str(page_size)+"&filter="+str(filter)
                }
              }
        }
        
        # return D
        return D, 200


parser2 = api.parser()
parser2.add_argument("by", type=str)
parser2.add_argument("format", type=str)

@api.response(200, 'OK')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
@api.response(201, 'Created')
@api.param('by','parameter to find proportion')
@api.param('format','json or image')
@api.route('/tv-shows/statistics')
class TV4(Resource):
    @api.doc(description="Get the statistics of the existing TV shows")
    def get(self):
        # Q6
        # 
        from matplotlib import pyplot as plt
        import matplotlib.image as img
        from flask import send_file
        
        # check if id exists
        conn = sqlite3.connect('TVS.db')
        c = conn.cursor()
        
        c.execute('SELECT * FROM TVSHOWS')
        row = c.fetchone()
        #print(row)
        if row == None:
            api.abort(404, "no tv-shows exist")
        
        args = parser2.parse_args()

        # retrieve the query parameters
        by = args.get('by')
        format = args.get('format')
        
        # count of id to get total
        c.execute('SELECT COUNT(DISTINCT id) FROM TVSHOWS;')
        total = c.fetchone()
        
        if format=='image' and by=='genres':
            # set of all genres
            by = 'genres'
            # get the proportions of shows ordered by the argument "by"
            c.execute('''SELECT
                            '''+str(by)+'''
                        FROM
                            TVSHOWS
                        ;''')
            rows = c.fetchall()
            
            # set of all genres
            allgen = set()
            for row in rows:
                for genre in eval(row[0]):
                    allgen.add(genre)
            
            # create dictionary of all genre
            dictionary = dict()
            for i in allgen:
                dictionary.update({i:0})
                
            # add count to dictionary
            for row in rows:
                for genre in eval(row[0]):
                    dictionary[genre]+=1
            
            # divide all genre counts by total shows to get fraction
            for i in dictionary.keys():
                dictionary[i] = dictionary[i]/total[0]*100
                
            dictionary = {k: v for k, v in sorted(dictionary.items(), key=lambda item: item[1])}
            x = list(dictionary.keys())
            y = list(dictionary.values())
            
            #plt.barh(x, width=y)
            f, ax = plt.subplots(figsize=(12,8))
            ax.barh(x, width=y)
            ax.set_title('Shows Proportion by '+str(by))
            ax.set_xlabel("% of shows")
            f.savefig('fig.png')
            im = img.imread('fig.png')
            
            return send_file('fig.png', mimetype='image/png', attachment_filename='fig.png', cache_timeout=0)
            
        # get the number of shows updated in the last 24h
        now = str(datetime.datetime.now())[:-7]
        now = now[:9]+str(int(now[9])-1)+now[10:]
        c.execute('SELECT COUNT(DISTINCT id) FROM TVSHOWS WHERE lastupdate > "'+now+'" ORDER BY lastupdate DESC;')
        updated24 = c.fetchall()
        
        
        # get the proportions of shows ordered by the argument "by"
        c.execute('''SELECT
                        '''+str(by)+''',
                        COUNT(id)
                    FROM
                        TVSHOWS
                    GROUP BY
                        '''+by+''';''')
        rows = c.fetchall()
        
        # create dictionary for proportions
        d = dict()
        for i in rows:
            #print(i)
            #print(total)
            d.update({i[0]:(i[1]/total[0]*100)})
            
        ret = { 
               "total": total[0],
               "total-updated": updated24[0][0],
               "values" : d 
            }
        
        # if format==json return d otherwise return an image of the pie chart
        # if format==image return f
        f, ax = plt.subplots(figsize=(8,8))
        ax.pie(d.values(), labels=d.keys(), autopct='%1.0f%%')
        ax.set_title('Shows Proportion by '+str(by))
        
        f.savefig('fig.png')
        im = img.imread('fig.png')
        if format=='json':
            return ret, 200
        else:
            return send_file('fig.png', mimetype='image/png', attachment_filename='fig.png', cache_timeout=0)



if __name__ == '__main__':
    
    
    
    # create sqlite database
    conn = sqlite3.connect('TVS.db')
    c = conn.cursor()
    
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS TVSHOWS (
        ID     INTEGER NOT NULL PRIMARY KEY,
        tvmazeid      INTEGER UNIQUE,
        lastupdate     TEXT,
        name    TEXT,
        type    TEXT,
        language    TEXT,
        genres    TEXT,
        status    TEXT,
        runtime   TEXT,
        premiered   TEXT,
        officialSite     TEXT,
        schedule    TEXT,
        rating    INTEGER,
        weight    INTEGER,
        network    TEXT,
        summary    TEXT,
        links     TEXT
        );
        """
    )
    
    c.close()
    conn.close()

    
    # run the application
    app.run(debug=True, port=5005)
