Review data (1 csv)

asin - ID of the product, like B000FA64PK (String)
helpful - helpfulness rating of the review - example: 2/3. (String)
overall - rating of the product. (int)
reviewText - text of the review (heading). (String)
reviewTime - time of the review (raw). (Date)
reviewerID - ID of the reviewer, like A3SPTOKDG7WBLN 
reviewerName - name of the reviewer. (String)
summary - summary of the review (description). (String)
unixReviewTime - unix timestamp. (int)

Sales data

amazon_com.csv

asin - ID of the product, like B000FA64PK (String)
group - is either "book" or "kindle", the two categories in terms of sales ranking.
format - is the specific book format, lowercase.

amazon_com_extras.csv

asin - ID of the product, like B000FA64PK (String)
group - is either "book" or "kindle", the two categories in terms of sales ranking.
format - is the specific book format, lowercase.
TITLE (String)
AUTHOR (String)
PUBLISHER (String)

Ranks data (JSON)

asin - ID of the product, like B000FA64PK (String)
SalesRank