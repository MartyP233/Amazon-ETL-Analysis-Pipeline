import pandas as pd
import os

def pre_process_salesrank(input_path, output_path):
    """Processes JSON files in prep for uploading to s3.
    
    Takes a folder of JSON files, adds their filename
    an extra field, and saves the file out to csv.
    """
    files = [pos_json for pos_json in os.listdir(input_path) if pos_json.endswith('.json')]
    # files = os.listdir(input_path)
    asins = list(map(lambda each:each.strip("_com_norm.json"), files))

    if not os.path.exists(output_path):
        os.mkdir(output_path)

    seq = [i for i in range(1669, 66760+1, 1669)]
    
    big_df =  pd.DataFrame(columns=['asin', 'rank'])
    
    for asin, filename in zip(asins, files):
            try:
                df = pd.read_json(os.path.join(input_path, filename), typ='series')
                df = df.to_frame(name='rank')
                df = df.assign(asin=asin)
                big_df = big_df.append(df, sort=False)
                print(asins.index(asin))
            except:
                print(f"failed to process {filename}")
                continue
            if asins.index(asin) in seq:
                        big_df.index.name = 'timestamp'
                        big_df.to_csv(f"{output_path}/{asins.index(asin)}.csv")
                        print(asins.index(asin))
                        big_df =  pd.DataFrame(columns=['asin', 'rank'])

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
    print("Processing salesrank data")
    pre_process_salesrank('Data/unzipped/ranks_norm','Data//processed/ranks_norm')
    print("Processing books data")
    pre_process_books('Data/unzipped/amazon_com_extras.csv','Data/processed/amazon_com_extras_processed.csv')
    print("Processing reviews data")
    pre_process_reviews('Data/unzipped/kindle_reviews.csv', 'Data/processed/kindle_reviews_processed.csv')

if __name__ == "__main__":
    main()