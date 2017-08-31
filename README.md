The application revolves movies dataset. The dataset contains 4 files which are follows,


File Name	Description / Schema
	
movies.dat	MovieID – Title – Genres
	
ratings.dat	UserID – MovieID – Rating – Timestamp
	
users.dat	UserID – Gender – Age – Occupation – ZipCode
	
The code solves below problems.

1.	Top ten most viewed movies with their movies Name (Ascending or Descending order)  

2.	Top twenty rated movies (Condition: The movie should be rated/viewed by at least 40 users) 

3.	We wish to know how have the genres ranked by Average Rating, for each profession and age group. The age groups to be considered are: 18-35, 36-50 and 50+. 

Formulate results in following table:

Genre Ranking by Avg. Rating	
Occupation	Age-Group Rank 1	Rank 2	Rank 3	Rank 4	Rank 5
Programmer	18-35	Action	Suspense	Thriller	Romance	Horror
Programmer	36-50	Action	Suspense	Thriller	Romance	Horror
Programmer	50+	Action	Suspense	Thriller	Romance	Horror
Farmer	18-35	Action	Suspense	Thriller	Romance	Horror
Farmer	36-50	Action	Suspense	Thriller	Romance	Horror

Note that values populated in following table are just representative, and will change with the actual data.


The result set populated here: 
https://github.com/tamizh-git/apache-spark/blob/master/movies-app/movies-coding-problem-report.xls
