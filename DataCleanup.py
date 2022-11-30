import csv

mRead = open('rotten_tomatoes_movies.csv', newline='')
mWrite = open('rotten_tomatoes_movies.txt', "w")
reader = csv.DictReader(mRead)
for row in reader:
    movieId = row['rotten_tomatoes_link']
    title = row['movie_title']
    info = row['movie_info']
    rating = row['content_rating']
    genres = row['genres']
    actors = row['actors']
    releaseDate = row['original_release_date']
    runtime = row['runtime']
    productionCompany = row['production_company']
    tomatoRating = row['tomatometer_rating']
    audienceRating = row['audience_rating']
    mWrite.write(
        f"{movieId}~~{title}~~{info}~~{rating}~~{genres}~~{actors}~~{releaseDate}~~{runtime}~~{productionCompany}~~{tomatoRating}~~{audienceRating}\n")

rRead = open("rotten_tomatoes_critic_reviews.csv", newline='')
rWrite = open("rotten_tomatoes_critic_reviews.txt", "w")
rReader = csv.DictReader(rRead)
for row in rReader:
    movieId = row['rotten_tomatoes_link']
    criticName = row['critic_name']
    topCritic = row['top_critic']
    reviewScore = row['review_score']
    reviewContent = row['review_content']
    rWrite.write(
        f"{movieId}~~{criticName}~~{topCritic}~~{reviewScore}~~{reviewContent}\n"
    )

# Movies: Id, title, description(info), content rating, genre, actors, original release date, runtime, production company, tomatometer fresh rating, audience rating
# Reviews: Id, critic name, top critic, review score, review content