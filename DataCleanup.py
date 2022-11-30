import csv

mRead = open('rotten_tomatoes_movies.csv', newline='')
mWrite = open('rotten_tomatoes_movies.txt', "w")
reader = csv.DictReader(mRead)
for row in reader:
    movieId = row['rotten_tomatoes_link']
    if len(movieId) == 0:
        movieId = "null"
    title = row['movie_title']
    if len(title) == 0:
        title = "null"
    info = row['movie_info']
    if len(info) == 0:
        info = "null"
    rating = row['content_rating']
    if len(rating) == 0:
        rating = "null"
    genres = row['genres']
    if len(genres) == 0:
        genres = "null"
    actors = row['actors']
    if len(actors) == 0:
        actors = "null"
    releaseDate = row['original_release_date']
    if len(releaseDate) == 0:
        releaseDate = "null"
    runtime = row['runtime']
    if len(runtime) == 0:
        runtime = "null"
    productionCompany = row['production_company']
    if len(productionCompany) == 0:
        productionCompany = "null"
    tomatoRating = row['tomatometer_rating']
    if len(tomatoRating) == 0:
        tomatoRating = "null"
    audienceRating = row['audience_rating']
    if len(audienceRating) == 0:
        audienceRating = "null"
    mWrite.write(
        f"{movieId}~~{title}~~{info}~~{rating}~~{genres}~~{actors}~~{releaseDate}~~{runtime}~~{productionCompany}~~{tomatoRating}~~{audienceRating}\n")

rRead = open("rotten_tomatoes_critic_reviews.csv", newline='')
rWrite = open("rotten_tomatoes_critic_reviews.txt", "w")
rReader = csv.DictReader(rRead)
for row in rReader:
    movieId = row['rotten_tomatoes_link']
    if len(movieId) == 0:
        movieId = "null"
    criticName = row['critic_name']
    if len(criticName) == 0:
        criticName = "null"
    topCritic = row['top_critic']
    if len(topCritic) == 0:
        topCritic = "null"
    reviewScore = row['review_score']
    if len(reviewScore) == 0:
        reviewScore = "null"
    reviewContent = row['review_content']
    if len(reviewContent) == 0:
        reviewContent = "null"
    rWrite.write(
        f"{movieId}~~{criticName}~~{topCritic}~~{reviewScore}~~{reviewContent}\n"
    )

# Movies: Id, title, description(info), content rating, genre, actors, original release date, runtime, production company, tomatometer fresh rating, audience rating
# Reviews: Id, critic name, top critic, review score, review content