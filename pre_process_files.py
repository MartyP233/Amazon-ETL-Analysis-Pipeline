import pandas as pd
import os

def pre_process_salesrank(input_path, output_path):
    """Processes JSON files in prep for uploading to s3.
    
    Takes a folder of JSON files, adds their filename
    an extra field, and saves the file out to csv.
    """
    files = os.listdir(input_path)
    asins = list(map(lambda each:each.strip("_com_norm.json"), files))

    if not os.path.exists(output_path):
        os.mkdir(output_path)

    for asin, filename in zip(asins, files):
        df = pd.read_json(os.path.join(input_path, filename), typ='series')
        df = df.to_frame(name='rank')
        df.index.name = 'timestamp'
        df = df.assign(asin=asin)
        df.to_csv(f"{output_path}/{asin}.csv")


def pre_process_books(csv, outputname):
    """Process books csv, removes rows with extra quotes.
    """
    # TODO: error with quotes within a field, those lines are getting skipped
    df = pd.read_csv(csv, error_bad_lines=False)
    df.to_csv(outputname, index=False)


def pre_process_reviews(csv, outputname):
    """Process reviews csv, removing index row.
    """
    df = pd.read_csv(csv)
    df = df.drop("Unnamed: 0", axis='columns')
    df.to_csv(outputname, index=False)


def main():

    pre_process_salesrank('Data/ranks_norm','Data/ranks_norm/processed')
    pre_process_books('Data/amazon-sales-rank-data-for-print-and-kindle-books/amazon_com_extras.csv','Data/processed/amazon_com_extras_processed.csv')
    pre_process_reviews('Data/kindle-reviews/kindle_reviews.csv', 'Data/processed/kindle_reviews_processed.csv')

if __name__ == "__main__":
    main()