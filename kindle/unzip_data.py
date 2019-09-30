import zipfile

# unzip local file

def unzip_file(zip_file, filename, directory):
    """Unzip a specific file within a zipped folder.
    """
    with zipfile.ZipFile(zip_file, 'r') as file:
        print(f'Extracting {filename}...')
        result = file.extract(filename, path=directory)
        print(f'Finished extracting {filename} at {result}')

def main():
    unzip_file('Data/downloads/kindle-reviews.zip', 'kindle_reviews.csv', 'Data/unzipped/')
    unzip_file('Data/downloads/amazon-sales-rank-data-for-print-and-kindle-books.zip', 'amazon_com_extras.csv', 'Data/unzipped/')
    unzip_file('Data/downloads/amazon-sales-rank-data-for-print-and-kindle-books.zip', 'ranks_norm.zip', 'Data/unzipped/')

    zip_file = 'Data/unzipped/ranks_norm.zip'
    with zipfile.ZipFile(zip_file, 'r') as file:
        print(f"Extracting {zip_file}...")
        file.extractall(path='Data/unzipped/')

if __name__ == "__main__":
    main()
