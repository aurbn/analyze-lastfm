#!/usr/bin/env python
# encoding: utf8

import sys
import re
import os
import os.path
import json
import cjson
from subprocess import check_call
from time import sleep, mktime
from datetime import datetime
from collections import namedtuple, defaultdict, Counter
from itertools import islice
from xml.etree import ElementTree
from hashlib import md5

import requests
requests.packages.urllib3.disable_warnings()

import seaborn as sns
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib import rc
# For cyrillic labels
rc('font', family='Verdana', weight='normal')

from skimage import io
import numpy as np


LASTFM_DIR = 'lastfm'
LASTFM_API = 'http://ws.audioscrobbler.com/2.0/'
LASTFM_KEY = 'bd221fa33740b25dcce42dac36c86b60'
LASTFM_TRACKS = 'lastfm_tracks.json'

ECHONEST_DIR = 'echonest'
ECHONEST_API = 'http://developer.echonest.com/api/v4/'
ECHONEST_KEY = 'L5WW5JLI1ZVGAPJQW'
ECHONEST_SERPS = 'echonest_serps.json'

MUSICBRAINZ_DIR = 'musicbrainz'
MUSICBRAINZ_API = 'http://musicbrainz.org/ws/2/'

COVERS_DIR = 'covers'
COVERS_GRID = 'covers.png'


LastfmArtist = namedtuple('LastfmArtist', ['name', 'image'])
LastfmAlbum = namedtuple('LastfmAlbum', ['name', 'image', 'mbid'])
LastfmTrack = namedtuple(
    'LastfmTrack',
    ['artist', 'album', 'name', 'timestamp', 'loved']
)

EchonestAudio = namedtuple(
    'EchonestAudio',
    ['energy', 'liveness', 'tempo', 'speechiness',
     'acousticness', 'danceability',
     'instrumentalness', 'duration', 'loudness']
)
EchonestTrack = namedtuple('EchonestTrack', ['artist', 'name', 'audio'])

MusicBrainzReleseRecord = namedtuple('MusicBrainzReleseRecord', ['year'])

ArtistTrack = namedtuple('ArtistTrack', ['artist', 'track'])
Album = namedtuple('Album', ['name', 'image', 'year'])
Track = namedtuple(
    'Track',
    ['artist', 'album', 'name', 'listened', 'audio']
)


def call_lastfm(**parameters):
    parameters['api_key'] = LASTFM_KEY
    response = requests.get(
        LASTFM_API,
        params=parameters
    )
    return response.content


def download_lastfm_tracks_page(page, user='AlexKuk'):
    print >>sys.stderr, 'Download lastfm tracks for {user}, page: {page}'.format(
        user=user,
        page=page
    )
    return call_lastfm(
        method='user.getRecentTracks',
        limit=200,
        page=page,
        user=user,
        extended=1,
    )


def get_lastfm_tracks_page_filename(page):
    return '{page}.xml'.format(page=page)


def parse_lastfm_tracks_page_filename(filename):
    page, _ = os.path.splitext(filename)
    return int(page)


def get_lastfm_tracks_page_path(page):
    filename = get_lastfm_tracks_page_filename(page)
    return os.path.join(LASTFM_DIR, filename)


def load_lastfm_tracks_page(page):
    path = get_lastfm_tracks_page_path(page)
    with open(path) as file:
        return file.read()


def dump_lastfm_tracks_page(data, page):
    path = get_lastfm_tracks_page_path(page)
    with open(path, 'w') as file:
        file.write(data)


def parse_timestamp(timestamp):
    if timestamp:
        return datetime.fromtimestamp(timestamp)


def parse_lastfm_tracks_page(data):
    xml = ElementTree.fromstring(data)
    for track in xml.find('recenttracks'):
        artist = track.find('artist')
        artist_name = artist.findtext('name')
        album_image = artist.findtext('image[@size="extralarge"]') or None
        loved = bool(int(track.findtext('loved')))
        name = track.findtext('name')
        album = track.find('album')
        album_name = album.text or None
        album_mbid = album.attrib['mbid'] or None
        artist_image = track.findtext('image[@size="extralarge"]')
        timestamp = track.find('date')
        if timestamp is not None:
            timestamp = timestamp.attrib['uts']
            timestamp = parse_timestamp(int(timestamp))
        yield LastfmTrack(
            LastfmArtist(artist_name, artist_image),
            LastfmAlbum(album_name, album_image, album_mbid),
            name, timestamp, loved
        )


def list_lastfm_tracks_pages():
    for filename in os.listdir(LASTFM_DIR):
        yield parse_lastfm_tracks_page_filename(filename)


def load_raw_lastfm_tracks():
    for index, page in enumerate(list_lastfm_tracks_pages()):
        if index > 0 and index % 100 == 00:
            print >>sys.stderr, 'Loading file #{index}'.format(
                index=index
            )
        data = load_lastfm_tracks_page(page)
        for track in parse_lastfm_tracks_page(data):
            yield track


def serialize_timestamp(timestamp):
    if timestamp:
        return mktime(timestamp.timetuple())


def dump_lastfm_tracks(tracks, path=LASTFM_TRACKS):
    with open(path, 'w') as file:
        data = [
            ((_.artist.name, _.artist.image),
             (_.album.name, _.album.image, _.album.mbid),
             _.name, serialize_timestamp(_.timestamp), _.loved)
            for _ in tracks]
        file.write(cjson.encode(data))


def load_lastfm_tracks(path=LASTFM_TRACKS):
    with open(path) as file:
        data = cjson.decode(file.read())
        return [
            LastfmTrack(
                LastfmArtist(artist_name, artist_image),
                LastfmAlbum(album_name, album_image, album_mbid),
                name, parse_timestamp(timestamp), loved
            )
            for ((artist_name, artist_image),
                 (album_name, album_image, album_mbid),
                 name, timestamp, loved)
            in data
        ]


def call_echonest(method, **parameters):
    parameters['api_key'] = ECHONEST_KEY
    parameters['format'] = 'json'
    response = requests.get(
        ECHONEST_API + method,
        params=parameters
    )
    return response.json()


def download_echonest_track_serp(query):
    artist, track = query
    print >>sys.stderr, u'Search at Echonest "{artist} - {track}"'.format(
        artist=artist,
        track=track
    )
    return call_echonest(
        'song/search',
        results=100,
        artist=artist,
        title=track,
        bucket=['audio_summary', 'artist_discovery',
                'artist_discovery_rank', 'artist_familiarity',
                'artist_familiarity_rank', 'artist_hotttnesss',
                'artist_hotttnesss_rank', 'artist_location',
                'song_currency', 'song_currency_rank', 'song_hotttnesss',
                'song_hotttnesss_rank', 'song_type']
    )


def get_artist_track_hash(query):
    hash = query.artist + query.track
    hash = md5(hash.encode('utf8')).hexdigest()
    return hash


def get_echonest_track_serp_filename(query):
    return '{hash}.json'.format(
        hash=get_artist_track_hash(query)
    )


def get_echonest_track_serp_path(query):
    filename = get_echonest_track_serp_filename(query)
    return os.path.join(ECHONEST_DIR, filename)


def load_echonest_track_serp(query):
    path = get_echonest_track_serp_path(query)
    with open(path) as file:
        return json.load(file)


def dump_echonest_track_serp(serp, query):
    path = get_echonest_track_serp_path(query)
    with open(path, 'w') as file:
        json.dump(serp, file)


def parse_echonest_track_serp(data):
    for track in data['response']['songs']:
        artist = track['artist_name']
        name = track['title']
        summary = track['audio_summary']
        energy = summary['energy']
        liveness = summary['liveness']
        tempo = summary['tempo']
        speechiness = summary['speechiness']
        acousticness = summary['acousticness']
        danceability = summary['danceability']
        instrumentalness = summary['instrumentalness']
        duration = summary['duration']
        loudness = summary['loudness']
        yield EchonestTrack(
            artist, name,
            EchonestAudio(
                energy, liveness, tempo, speechiness,
                acousticness, danceability,
                instrumentalness, duration, loudness
            )
        )


def get_track_artist_track(track):
    return ArtistTrack(track.artist.name, track.name)


def load_echonest_serps(tracks):
    serps = {}
    queries = {get_track_artist_track(_) for _ in tracks}
    for index, query in enumerate(queries):
        if index > 0 and index % 2000 == 0:
            print >>sys.stderr, 'Parse serp #{index}'.format(
                index=index
            )
        data = load_echonest_track_serp(query)
        serp = list(parse_echonest_track_serp(data))
        serps[query] = serp
    return serps


def dump_echonest_serps(serps, path=ECHONEST_SERPS):
    with open(path, 'w') as file:
        data = [
            (
                tuple(query),
                [
                    (track.artist, track.name, tuple(track.audio))
                    for track in serp
                ]
            )
            for query, serp in serps.iteritems()
        ]
        file.write(cjson.encode(data))


def load_echonest_serps(path=ECHONEST_SERPS):
    with open(path) as file:
        data = cjson.decode(file.read())
        return {
            ArtistTrack(*query): [
                EchonestTrack(
                    artist, track,
                    EchonestAudio(*audio)
                )
                for artist, track, audio in serp
            ]
            for query, serp in data 
        }


def call_musicbrainz(*path, **parameters):
    parameters['fmt'] = 'json'
    response = requests.get(
        os.path.join(MUSICBRAINZ_API, *path),
        params=parameters
    )
    return response.json()


def download_musicbrainz_release(mbid):
    print >>sys.stderr, 'Download musicbrainz release info for {mbid}'.format(
        mbid=mbid
    )
    return call_musicbrainz('release', mbid)


def get_musicbrainz_release_filename(mbid):
    return '{mbid}.json'.format(mbid=mbid)


def parse_musicbrainz_release_filename(filename):
    mbid, _ = os.path.splitext(filename)
    return mbid


def get_musicbrainz_release_path(mbid):
    filename = get_musicbrainz_release_filename(mbid)
    return os.path.join(MUSICBRAINZ_DIR, filename)


def load_musicbrainz_release(mbid):
    path = get_musicbrainz_release_path(mbid)
    with open(path) as file:
        return json.load(file)


def dump_musicbrainz_release(data, mbid):
    path = get_musicbrainz_release_path(mbid)
    with open(path, 'w') as file:
        json.dump(data, file)


def parse_musicbrainz_release(data):
    year = None
    if 'date' in data:
        date = data['date']
        match = re.search('^(\d{4})', date)
        if match:
            year = match.group(1)
            year = int(year)
    return MusicBrainzReleseRecord(year)


def list_musicbrainz_releases():
    for filename in os.listdir(MUSICBRAINZ_DIR):
        yield parse_musicbrainz_release_filename(filename)


def load_musicbrainz_releases():
    releases = {}
    for mbid in list_musicbrainz_releases():
        data = load_musicbrainz_release(mbid)
        release = parse_musicbrainz_release(data)
        releases[mbid] = release
    return releases


def join_lastfm_echonest(tracks, serps, releases):
    for track in tracks:
        serp = serps.get(get_track_artist_track(track))
        audio = None
        if serp:
            best = serp[0]
            audio = best.audio
        album = track.album
        mbid = album.mbid
        year = None
        if mbid is not None and mbid in releases:
            year = releases[mbid].year
        album = Album(album.name, album.image, year)
        yield Track(
            track.artist,
            album,
            track.name,
            track.timestamp,
            audio
        )


def filter_tracks_by_listened(tracks, start=datetime.strptime('2009-03-02', '%Y-%m-%d')):
    for track in tracks:
        listened = track.listened
        if listened and listened >= start:
            yield track


def show_tracks_by_time(tracks):
    table = pd.DataFrame([_.listened for _ in tracks], columns=['listened'])
    table = table.groupby('listened').size()
    table = table.resample('W', how='sum')
    fig, ax = plt.subplots()
    table.plot(ax=ax)
    ax.set_xlabel('')
    ax.set_ylabel('# tracks listened by weeks')


def get_top_artist_tracks(tracks):
    top = Counter(get_track_artist_track(_) for _ in tracks)
    for artist_track, _ in top.most_common():
        yield artist_track


def filter_tracks_by_artist_track(tracks, artist_tracks):
    artist_tracks = set(artist_tracks)
    for track in tracks:
        if get_track_artist_track(track) in artist_tracks:
            yield track


def shorten_string(string, top=20):
    if len(string) > top:
        return string[:top] + '...'
    else:
        return string


def show_tracks_by_artist_track_by_time(tracks, rows=5, columns=5, size=(20, 20)):
    table = pd.DataFrame(
        [(_.listened, _.artist.name, _.name) for _ in tracks],
        columns=['listened', 'artist', 'track']
    )
    fig, axis = plt.subplots(rows, columns)
    for (artist, track), ax in zip(get_top_artist_tracks(tracks), axis.flatten()):
        series = table[(table.artist == artist) & (table.track == track)]
        series = series.groupby('listened').size()
        series = series.resample('W', how='sum')
        series = series.fillna(0)
        title = u'{artist}\n{track}'.format(
            artist=shorten_string(artist),
            track=shorten_string(track)
        )
        series.plot(ax=ax, figsize=size, title=title)
    fig.tight_layout()


def get_top_artists(tracks):
    top = Counter(_.artist.name for _ in tracks)
    for artist, _ in top.most_common():
        yield artist


def filter_tracks_by_artists(tracks, artists):
    artists = set(artists)
    for track in tracks:
        if track.artist.name in artists:
            yield track


def show_tracks_by_artist_by_time(tracks, rows=5, columns=5, size=(20, 20)):
    table = pd.DataFrame(
        [(_.listened, _.artist.name) for _ in tracks],
        columns=['listened', 'artist']
    )
    fig, axis = plt.subplots(rows, columns)
    for artist, ax in zip(get_top_artists(tracks), axis.flatten()):
        series = table[table.artist == artist]
        series = series.groupby('listened').size()
        series = series.resample('W', how='sum')
        series = series.fillna(0)
        title = shorten_string(artist)
        series.plot(ax=ax, figsize=size, title=title)
    fig.tight_layout()


def format_artist_track(artist_track):
    return u'{0.artist} — {0.track}'.format(artist_track)


def show_selected_tracks_artists(tracks, artist_tracks, artists,
                                 rows=5, columns=5, width=20, height=20):
    data = defaultdict(Counter)
    for track in tracks:
        listened = track.listened
        artist_track = get_track_artist_track(track)
        if artist_track in artist_tracks:
            data[artist_track][listened] += 1
        artist = track.artist.name
        if artist in artists:
            data[artist][listened] += 1
    names = artist_tracks + artists
    dates = [date for name in names for date in data[name]]
    fig, axis = plt.subplots(rows, columns)
    for name, ax in zip(names, axis.flatten()):
        series = pd.Series({date: data[name][date] for date in dates})
        series = series.resample('W', how='sum')
        series = series.fillna(0)
        if type(name) is ArtistTrack:
            title = u'{artist}\n{track}'.format(
                artist=shorten_string(name.artist),
                track=shorten_string(name.track)
            )
        else:
            title = shorten_string(name)
        series.plot(ax=ax, figsize=(width, height), title=title)
        ax.set_ylabel('# tracks per week')
    fig.tight_layout() 


def get_listened_first_time(tracks, window, get_name=get_track_artist_track):
    day_names = defaultdict(list)
    for track in tracks:
        listened = track.listened
        day = datetime(listened.year, listened.month, listened.day)
        name = get_name(track)
        day_names[day].append(name)
    days = sorted(day_names)
    day_first_times = Counter()
    if window is not None:
        for index in xrange(1, len(days)):
            day = days[index]
            index_ = index - 1
            day_ = days[index_]
            listened = set()
            while (day - day_).days <= window and index_ >= 0:
                listened.update(day_names[day_])
                index_ -= 1
                day_ = days[index_]
            first_times = 0
            for name in day_names[day]:
                if name not in listened:
                    first_times += 1
            day_first_times[day] = first_times
    else:
        listened = set(day_names[days[0]])
        for day in days[1:]:
            first_times = 0
            names = day_names[day]
            for name in names:
                if name not in listened:
                    first_times += 1
            day_first_times[day] = first_times
            listened.update(names)
    return day_first_times


def show_day_first_times(
    tracks,
    get_name=get_track_artist_track,
    ylabel='share of tracks played first time (avg. by months)'
):
    first_time_in_week = get_listened_first_time(tracks, window=7, get_name=get_name)
    first_time_in_month = get_listened_first_time(tracks, window=30, get_name=get_name)
    first_time_in_6_months = get_listened_first_time(tracks, window=120, get_name=get_name)
    first_time_ever = get_listened_first_time(tracks, window=None, get_name=get_name)
    table = pd.DataFrame({
        'in_week': first_time_in_week,
        'in_month': first_time_in_month,
        'in_6_months': first_time_in_6_months,
        'in_all_time': first_time_ever
    })
    total = get_listened_first_time(tracks, window=0)
    table = table.div(pd.Series(total), axis=0)
    table = table.resample('M', how='mean')
    table = table.reindex(columns=['in_week', 'in_month', 'in_6_months', 'in_all_time'])
    fig, ax = plt.subplots()
    table.plot(cmap='Blues', ylim=(0, 1), ax=ax)
    ax.set_ylabel(ylabel)


def show_day_listen_repetitions(
    tracks, get_name=get_track_artist_track,
    ylabel='share of track freq. per day avg. by months'
):
    day_names = defaultdict(list)
    for track in tracks:
        listened = track.listened
        day = datetime(listened.year, listened.month, listened.day)
        name = get_name(track)
        day_names[day].append(name)
    data = []
    totals = {}
    for day, names in day_names.iteritems():
        total = len(names)
        repetitions = Counter(names)
        repetition_frequencies = Counter()
        for count in repetitions.itervalues():
            repetition_frequencies[count] += count
        data.append({
            'date': day,
            '1': repetition_frequencies[1],
            '[2, 5)': sum(repetition_frequencies[_] for _ in xrange(2, 5)),
            '[5, 10)': sum(repetition_frequencies[_] for _ in xrange(5, 10)),
        })
        totals[day] = total
    table = pd.DataFrame(data)
    table = table.set_index('date')
    totals = pd.Series(totals)
    table['[10, inf)'] = totals - table.sum(axis=1)
    table = table.div(totals, axis=0)
    table = table.resample('M', how='mean')
    fig, ax = plt.subplots()
    table.plot(kind='area', cmap='Blues', ylim=(0, 1), ax=ax)
    ax.set_ylabel(ylabel)


def show_year_coverage_by_time(tracks):
    data = [(_.listened, _.album.year is not None) for _ in tracks]
    table = pd.DataFrame(data, columns=['listened', 'year'])
    table = table.groupby(['listened', 'year']).size()
    table = table.unstack()
    table = table.resample('w', how='sum')
    table = table.div(table.sum(axis=1), axis=0)
    fig, ax = plt.subplots()
    table[True].plot(ax=ax)
    ax.set_xlabel('')
    ax.set_ylabel('share of tracks found in musicbrainz db')


def show_album_year_by_time(tracks):
    data = [(_.listened, _.album.year) for _ in tracks]
    table = pd.DataFrame(data, columns=['listened', 'year'])
    table = table.set_index('listened').year
    table = table.resample('w', how='mean')
    fig, ax = plt.subplots()
    table.plot(ylim=(2005, None), ax=ax)
    # Disable scientific notation for y axis
    ax.ticklabel_format(useOffset=False, axis='y')
    ax.set_xlabel('')
    ax.set_ylabel('mean year of tracks release')


def show_echonest_coverage_by_time(tracks):
    data = [(_.listened, _.audio is not None) for _ in tracks]
    table = pd.DataFrame(data, columns=['listened', 'echonest'])
    table = table.groupby(['listened', 'echonest']).size()
    table = table.unstack()
    table = table.resample('w', how='sum')
    table = table.div(table.sum(axis=1), axis=0)
    table[True].plot()


def get_audio_table(tracks):
    data = [(_.listened, _.audio.energy, _.audio.liveness,
              _.audio.tempo, _.audio.speechiness, _.audio.acousticness,
              _.audio.danceability, _.audio.instrumentalness,
              _.audio.duration, _.audio.loudness) for _ in tracks if _.audio
    ]
    table = pd.DataFrame(
        data,
        columns=[
            'listened', 'energy', 'liveness', 'tempo', 'speechiness',
            'acousticness', 'danceability', 'instrumentalness', 'duration', 'loudness'
        ]
    )
    table = table.set_index('listened')
    return table


def show_audio_by_time(tracks):
    table = get_audio_table(tracks)
    table = table.resample('M', how='mean')
    table.plot(subplots=True, figsize=(15, 15), layout=(4, -1))


def show_selected_tracks_audio_by_time(tracks):
    table = get_audio_table(tracks)
    fig, axis = plt.subplots(2, 2)
    for feature, ax in zip(
        ['liveness', 'speechiness', 'danceability', 'instrumentalness'],
        axis.flatten()
    ):
        series = table[feature]
        series = series.resample('w', how='mean')
        series.plot(figsize=(12, 8), ax=ax, title=feature)
        ax.set_xlabel('')
        ax.set_ylabel('mean feature value by week')
    fig.tight_layout()


def get_top_cover_urls(tracks):
    top = Counter(_.album.image for _ in tracks if _.album.image is not None)
    for url, _ in top.most_common():
        yield url


def download_cover(url):
    print >>sys.stderr, 'Download {url}'.format(url=url)
    check_call(['wget', url], cwd=COVERS_DIR)


def read_covers():
    for filename in os.listdir(COVERS_DIR):
        if filename.endswith('.png'):
            path = os.path.join(COVERS_DIR, filename)
            cover = io.imread(path)
            if cover.shape == (300, 300, 4):
                yield cover

def build_covers_grid(rows=6, columns=9):
    covers = read_covers()
    grid = []
    for row in xrange(rows):
        grid_row = []
        for column in xrange(columns):
            cover = next(covers)
            grid_row.append(cover)
        grid.append(grid_row)
    rows = []
    for row in grid:
        row = np.concatenate(row, axis=1)
        rows.append(row)
    image = np.concatenate(rows, axis=0)
    io.imsave(COVERS_GRID, image)
