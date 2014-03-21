Please run the following commands on hadoop configured machine for each of the problem numbers specified below.




1) hadoop jar Rating.jar Rating inputpath/ratings.dat XXX YYY
XXX - Output directory for 1st question. YYY - Age in the question.

2) hadoop jar Genres.jar Genres inputpath/movies.dat XXX "ZZZ"
XXX - Output directory for 2nd problem. ZZZ - Movie names(s) separated by comma.

3) hadoop jar Average.jar Average inputpath/users.dat XXX
XXX - output directory for 3rd problem.