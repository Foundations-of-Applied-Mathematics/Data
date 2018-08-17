# get_movie_data.py
"""Use imdbpy (pip install imdbpy) to get data for the Kevin Bacon lab."""

import os
import re
import pickle
from time import time
from statistics import mean
from collections import defaultdict

import imdb
try:
    from tqdm import tqdm
except ImportError:
    print("Recommended: pip install tqdm")
    tqdm = lambda x: x

# Global variables ============================================================

# Regular expressions for cleaning movie / actor data.
PROFANE = re.compile(r"""Sex|[Dd]amn|[Bb]itch|[Ss]hit|[Ff]uck|[Bb]astard|
                         [Ee]rotic|[Pp]enis|[Vv]agina|[Sp]ank""", re.VERBOSE)
FLIPPED_TITLE = re.compile(r"^(.+?), (The|A) (\(\d{4}\))")
FLIPPED_NAME = re.compile(r"(.+), (.+)")

BAD_TITLE = lambda t: "(None)" in t or t == "Biography (2002)"
BAD_IDS = "bad.pkl" # TODO: move this into the class.

TOP_ACTORS = [
  'Katharine Hepburn',
  'Jack Nicholson',
  'Laurence Olivier',
  'Meryl Streep',
  'Humphrey Bogart',
  'Audrey Hepburn',
  'Robert De Niro',
  'Bette Davis',
  'Cary Grant',
  'Marlon Brando',
  'James Stewart',
  'Ingrid Bergman',
  'Marilyn Monroe',
  'Al Pacino',
  'Grace Kelly',
  'Orson Welles',
  'Sidney Poitier',
  'Paul Newman',
  'Spencer Tracy',
  'Greta Garbo',
  'Alec Guinness',
  'Charles Chaplin',
  'Joan Crawford',
  'Henry Fonda',
  'Faye Dunaway',
  'Lillian Gish',
  'Gregory Peck',
  'Vivien Leigh',
  'Claudette Colbert',
  'Dustin Hoffman',
  'Tom Hanks',
  'Elizabeth Taylor',
  'Anthony Hopkins',
  'Barbara Stanwyck',
  'Olivia de Havilland',
  'Yul Brynner',
  'James Cagney',
  'Jack Lemmon',
  'Denzel Washington',
  'Joan Fontaine',
  'Sophia Loren',
  'Peter Sellers',
  'Judy Garland',
  'Kate Winslet',
  'Gary Cooper',
  'Edward G. Robinson',
  'Marlene Dietrich',
  'Charles Laughton',
  'Mae West',
  'Diane Keaton',
  'William Holden',
  'Daniel-Lewis Day',
  'Jodie Foster',
  'Julia Roberts',
  'Lauren Bacall',
  'Fred Astaire',
  'John Wayne',
  'Rita Hayworth',
  'Mifune, Toshirô',
  'Clark Gable',
  'Shirley Temple',
  'Buster Keaton',
  'Judi Dench',
  'Deborah Kerr',
  "Maureen O'Hara",
  'Michael Caine',
  'Gloria Swanson',
  'Kirk Douglas',
  'The Marx Brothers',
  'Susan Sarandon',
  'Gene Hackman',
  'Natalie Wood',
  'Jean Arthur',
  'Morgan Freeman',
  "Peter O'Toole",
  'Ben Kingsley',
  'Susan Hayward',
  'Setsuko Hara',
  'Robert Mitchum',
  'Burt Lancaster',
  'Robert Redford',
  'George C. Scott',
  'Jane Fonda',
  'Ellen Burstyn',
  'Charlton Heston',
  'Max von Sydow',
  'Barbra Streisand',
  'Kevin Spacey',
  'Clint Eastwood',
  'Ava Gardner',
  'Robert Duvall',
  'Sean Connery',
  'Luise Rainer',
  'Richard Burton',
  'Nicole Kidman',
  'Cate Blanchett',
  'Geoffrey Rush',
  'Angelina Jolie',
  'Ralph Fiennes',
  'Takashi Shimura'
]

# IMDB parser.
ia = imdb.IMDb(adultSearch=False)

# Helper Functions ============================================================

def ismovie(movie):
    return "kind" in movie.keys() and movie["kind"] == "movie"

def getname(person):
    keys = person.keys()
    for label in ["long imdb name", "name",
                  "long imdb canonical name", "canonical name"]:
        if label in keys:
            return person[label]
    raise KeyError(repr(person) + " has no name!")

def search_person(person_name):
    """Get the imdpy.Person object corresponding to 'person_name'."""
    matches = ia.search_person(person_name)
    found = None
    for match in matches:
        person = ia.get_person(match.personID)
        keys = person.keys()
        if "actor" in keys or "actress" in keys:
            found = person
            break
    if not found:
        raise ValueError("No people matching '{}'".format(person_name))
    return found

def search_movie(movie_title):
    """Get the imdbpy.Movie object corresponding to 'movie_title'."""
    matches = ia.search_movie(movie_title)
    found = None
    for match in matches:
        movie = ia.get_movie(match.movieID)
        if ismovie(movie):
            found = movie
            break
    if not found:
        raise ValueError("No movies matching '{}'".format(movie_title))
    return found

# =============================================================================

class MovieDatabase:
    """Object for holding info on movies and actors."""

    def __init__(self, loadprefix=None):
        """Initialize movies / actors dictionaries."""
        self.movies = {}                # movieID -> movie name
        self.actors = defaultdict(list) # movieID -> list of (ID, actor) tuples
        self._failures = set()

        # Load information about movie IDs to skip.
        self._bad_ids = set()
        if os.path.isfile(BAD_IDS):
            with open(BAD_IDS, 'rb') as infile:
                self._bad_ids.update(pickle.load(infile))

        # Load previous data if desired.
        if loadprefix:
            self.load(loadprefix)

        print(self)

    # Adders / Removers -------------------------------------------------------
    def add_filmography(self, person_name):
        """Add every movie that a specified person has ever been in.

        Parameters:
            person_name (str): an actor name to search for in imdbpy.
        """
        self._add_person(search_person(person_name).personID)

    def add_movie(self, movie_title):
        """Add a single movie to the database.

        Parameters:
            movie_name (str): an movie title to search for in imdbpy.
        """
        self._add_movie(search_movie(movie_title).movieID)

    def _add_person(self, person_ID):
        """Add every movie that a specified person has ever been in.

        Parameters:
            person_ID (str): an imdbpy.Person.personID.
        """
        person = ia.get_person(person_ID)
        keys = person.keys()
        if "actor" in keys:
            filtered = [m for m in person["actor"] if ismovie(m)]
        elif "actress" in keys:
            filtered = [m for m in person["actress"] if ismovie(m)]
        else:
            filtered = []
            print("ERROR: Person {} has no 'actor' or 'actress' key".format(
                                                                    person_ID))
        if filtered:
            for mov in filtered:
                self._add_movie(mov)

    def _add_movie(self, movie):
        """Add an (mID,title) tuple to the movies dictionary and the
        corresponding (mID,[(pID,cast)]) tuple to the actors dictionary.
        Note that duplicates are skipped and errors are suppressed.

        Parameters:
            movie (imdbpy.Movie): the actual movie object with info to add.
        """
        # Avoid repeating work.
        if movie.movieID in self.movies or movie.movieID in self._bad_ids:
            return

        try:
            # Add the movie and its title.
            if "long imdb title" not in movie.keys():
                print("\t{} has no 'long imdb title'".format(movie))
                return
            title = movie["long imdb title"]
            print("\tNew movie {}...".format(repr(title)), end='', flush=True)
            self.movies[movie.movieID] = title

            # Add the cast of the movie.
            cast = ia.get_movie_full_credits(movie.movieID)["data"]["cast"]
            for person in cast:
                # Add them to the list of actors in this movie.
                try:
                    self.actors[movie.movieID].append((person.personID,
                                                       getname(person)))
                except KeyError as e:
                    # print(e)
                    continue
            print("done", flush=True)
        except Exception:
            # Remove this movie / actors if there is a problem.
            print("failed to load movie with ID",movie.movieID, flush=True)
            self.remove_movie(movie.movieID)
            self._failures.add(movie.movieID)
        except KeyboardInterrupt:
            self.remove_movie(movie.movieID)
            raise

    def remove_movie(self, movie_ID):
        """Remove the movie with the given ID from the database.
        Do nothing if the movieID is invalid.

        Parameters:
            movieID (str): an imdby.Movie.movieID.
        """
        if movie_ID in self.movies:
            self.movies.pop(movie_ID)
        if movie_ID in self.actors:
            self.actors.pop(movie_ID)

    # Main Routines -----------------------------------------------------------
    def add_top250_movies(self):
        """Get data from the top 250 movies."""
        for mov in [m for m in ia.get_top250_movies() if ismovie(m)]:
            self._add_movie(mov)

    def add_top100_actors(self):
        """Get data from the top 100 actors."""
        N = len(TOP_ACTORS)
        for i,actor in enumerate(TOP_ACTORS):
            print("{}/{}: {}".format(i+1, N, actor))
            self.add_filmography(actor)

    def bfs_search(self, depth=10):
        """Add the first 'depth' actors billed in the top 250 movies."""
        self._failures.clear()
        try:
            # Get the top 'depth' actors in each of the current movies.
            unique_actors = set()
            for movie_ID in [m.movieID for m in ia.get_top250_movies()
                                                        if ismovie(m)]:
                unique_actors.update(self.actors[movie_ID][:depth])
            N = len(unique_actors)
            unique_actors = sorted(unique_actors, key=lambda x:x[1])

            # Add every movie those actors have been in.
            print("Adding {} actors".format(N))
            for i,person in enumerate(unique_actors):
                print("{}/{}: {}".format(i+1, N, person[1]))
                self._add_person(person[0])
        finally:
            if self._failures:
                print("Failed to load movies with the following IDs:\n\t",
                      "\n\t".join(self._failures))

    # I/O and persistence -----------------------------------------------------
    def load(self, prefix, clear=True):
        """Load the movies and actors dictionaries from pickle files.

        Parameters:
            prefix (str): the root of the files to load. That is, load from
                <prefix>.movies.pkl and <prefix>.actors.pkl.
            clear (bool): if True, clear existing data before loading.
        """
        prefix = prefix.split('.')[0]
        for label, target in zip(["movies", "actors"],
                                 [self.movies, self.actors]):
            if clear:
                target.clear()
            storage_file = "{}.{}.pkl".format(prefix, label)
            if os.path.isfile(storage_file):
                with open(storage_file, 'rb') as infile:
                    target.update(pickle.load(infile))

    def loadtxt(self, filename, clear=True):
        """Load the movies and actors dictionaries from a text file with format
        <movie 1>/<actor>/<actor>/ ... /<actor>
        <movie 2>/<actor>/<actor>/ ... /<actor>
        ...

        Parameters:
            filename (str): the complete name of the file to read from.
            clear (bool): if True, clear existing data before loading.
        """
        if clear:
            self.movies.clear()
            self.actors.clear()
        with open(filename, 'r') as infile:
            lines = infile.read().split('\n')
        while lines[-1] == '':
            lines.pop()
        for line in lines:
            cast = line.split('/')
            title = cast[0]
            cast = [x.strip() for x in cast[1:]]
            movieID = search_movie(title).movieID
            self.movies[movie_ID] = title
            self.actors[movie_ID] = cast

    def _txt_format(self, movie_ID):
        """Get a line of the form <title>/<actor>/<actor>/.../<actor> for a
        given movie ID.
        """
        title = self.movies[movie_ID] + '/'
        return title + '/'.join([a[1] for a in self.actors[movie_ID]])

    def export(self, prefix):
        """Export movies and actors dictionaries to a text file in the format
        <movie 1>/<actor>/<actor>/ ... /<actor>
        <movie 2>/<actor>/<actor>/ ... /<actor>
        ...,
        writing the top 250 movies first.
        Also save the movies and actors dictionaries in pickle (.pkl) files.

        Parameters:
            prefix (str): the root of the files to save. That is, generate
                <prefix>.txt, <prefix>.movies.pkl, and <prefix>.actors.pkl.
        """
        top250 = [m.movieID for m in ia.get_top250_movies()]
        prefix = prefix.split('.')[0]

        # Write the text file <prefix>.txt
        with open(prefix+".txt", 'w') as outfile:
            # Write the top 250 movies first
            for movie_ID in [m for m in top250 if m in self.movies]:
                outfile.write(self._txt_format(movie_ID) + '\n')
            # Write the rest of the movies next.
            top250 = set(top250)
            for movie_ID in self.movies:
                if movie_ID in top250:
                    continue
                outfile.write(self._txt_format(movie_ID) + '\n')

        # Pickle the movies and actors dictionaries.
        with open(prefix+".movies.pkl", 'wb') as outfile:
            pickle.dump(self.movies, outfile)
        with open(prefix+".actors.pkl", 'wb') as outfile:
            pickle.dump(self.actors, outfile)

    # TODO: Die hard (Hard, Die)? La Haine (Haine, La)
    def clean(self):
        """Remove bad movies and format movie titles and actor names."""
        initial_count = len(self.movies)
        print("Cleaning movie/actor database")

        bad = set()
        for movieID,cast in self.actors.items():
            for i, (actorID,name) in enumerate(cast):
                # Target movies with strange actor listings ("1 episode", etc.)
                if '\n' in name:
                    bad.add(movieID)
                    continue
                # Fix names: Bacon, Kevin --> Kevin Bacon
                if FLIPPED_NAME.match(name):
                    self.actors[movieID][i] = (actorID,
                                               FLIPPED_NAME.sub(r"\2 \1",name))
        strange_count = len(bad)
        print(strange_count, "movies with strange actor listings")

        # Target out movies with profane titles or no release date.
        for movieID,title in self.movies.items():
            if PROFANE.match(title) or BAD_TITLE(title):
                bad.add(movieID)
        print(len(bad) - strange_count, "movies with bad titles")

        # Filter out the bad movies.
        for movieID in bad:
            self.remove_movie(movieID)
        self._bad_ids.update(bad)
        for movieID,title in self.movies.items():
            # Fix titles: Rookie, The --> The Rookie
            if FLIPPED_TITLE.match(title):
                self.movies[movieID] = FLIPPED_TITLE.sub(r"\2 \1 \3", title)
            # Remove '/' from titles.
            if '/' in title:
                self.movies[movieID] = title.replace('/', '|')

        # Report cleaning results.
        print("{} movies -> {} movies".format(initial_count, len(self.movies)))

        # Save the IDs of bad movies to avoid adding them again.
        with open(BAD_IDS, 'wb') as outfile:
            pickle.dump(self._bad_ids, outfile)

        # # Print bad titles.
        YEAR = re.compile(r"\(\d{4}\)$")
        for movieID, title in self.movies.items():
            if not YEAR.match(title[-6:]):
                print(title)

    def __str__(self):
        return """MovieDatabase
            {} movies
            {} actors""".format(len(self.movies),
                                len({a[0] for cast in self.actors.values()
                                          for a in cast }))
# Main Routines ===============================================================

# TODO: simplify this so it isn't so dangerous.
def make_base(target="base", source=None):
    """Generate data on the top 250 movies + the filmographies of the top 100
    actors according to IMDb.

    Parameters:
        target (str): the prefix of the outfiles to produce.
        source (str): the prefix of existing base files (to add to).
    """
    mdb = MovieDatabase(source)

    try:
        # Add top 250 movies and top 100 actors.
        print("Adding top 250 movies")
        mdb.add_top250_movies()
        print("Adding top 100 actors")
        mdb.add_top100_actors()
    finally:
        # Clean and export results.
        mdb.clean()
        mdb.export(target)

def update_data(depth, source="movie_data"):
    """Update stuff"""
    mdb = MovieDatabase(source)
    try:
        mdb.bfs_search(depth=depth)
    finally:
        mdb.clean()
        mdb.export(source)

def test_data(filename="movie_data.txt"):
    """Make sure that we can still do the Kevin Bacon lab things."""
    import networkx as nx

    # Read the txt file and build a NetworkX graph.
    start = time()
    all_actors, all_movies = set(), set()
    G = nx.Graph()
    with open(filename, 'r') as infile:
            for line in infile:
                if line == '':
                    continue
                data = line.strip().split('/')
                title = data[0]
                cast = data[1:]
                all_actors.update(set(cast))
                all_movies.add(title)
                # G.add_node(title)
                # G.add_nodes_from(cast)
                G.add_edges_from([(title, person) for person in cast])
    print("Loaded graph in {:.2f} seconds".format(time() - start))

    def pathfinder(actor):
        paths = nx.shortest_path(G, actor)
        pathlengths = {name:len(path)//2 for name,path in paths.items()
                       if name in all_actors}
        return mean(pathlengths.values()), len(all_actors) - len(pathlengths)

    for actor in ["Kevin Bacon", "Mark Hamill", "Harrison Ford",
                  "Samuel L. Jackson", "Robert De Niro", "Zoua Kue"]:
        print(actor, pathfinder(actor))
    return G

if __name__ == '__main__':
    from sys import argv
    if "--make-base" in argv:
        make_base(target="base", source="base")
        # TODO: update_base?
    elif "--update" in argv:
        update_data(None, source="movie_data")
    elif "--test" in argv:
        test_data("movie_data.txt")
