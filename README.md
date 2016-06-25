USING SPARK SHELL

1 Generate movie name and its duration from the given dataset. 

Using this code below, I was able to generate the name and its duration

val input = sc.textFile(“movies.txt”)

val moviesnames = input.map(x => x.split(“,”)(1))

val moviesduration = input.map(x => x.split(“,”)(4))

moviesnames.collect

moviesduration.collect 

moviesnames :  The Nightmare Before Christmas, The Nightmare Before Christmas, The Nightmare Before Christmas, Orphans of the Storm, The Object of Beauty, Night Tide, Night Tide, Night Tide, One Magic Christmas, One Magic Christmas, Mother's Boys, Nosferatu, Nick of Time, Nosferatu. 

moviesduration: 4568, 4568, 4568, 4388, 9062, 6150, 5126, 5126, 5126, 5333, 6323, 5733 5651,5333, 5651






2 How many distinct movies are there?

Using this code below, I was able to generate the distinct movies and also count

val distinctmovies = moviesnames.distinct ()

val countmovies = distinctmovies.count()


distinctmovies.collect

countmovies.collect

distinctmovies:  The Nightmare Before Christmas, Orphans of the Storm, The Object of Beauty, Night Tide, One Magic Christmas,Mother's Boys, Nosferatu, Nick of Time, Nosferatu. 

countmovies: 9


	
